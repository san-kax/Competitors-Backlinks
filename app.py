\
"""
Streamlit app: Airtable → Ahrefs (last 14d backlinks) vs Gambling.com referring domains

What it does
- Pulls competitor domains (50–500) from a **primary Airtable** (with Geo info)
- Pulls additional domains from **4 other Airtable bases** (blocklist / suppression lists)
- Queries Ahrefs API for each competitor's NEW backlinks in the last N days (default 14)
- Extracts linking registrable domains from those new backlinks
- Builds/uses a cached set of ALL referring domains to Gambling.com (full historical), stored locally in SQLite
- Removes domains that exist in:
  • Gambling.com referring domains
  • Additional Airtable blocklists
- Exports CSV and shows a review table

Secrets / config (set these in Streamlit Cloud **Secrets** or env):
- AHREFS_API_TOKEN: Ahrefs v3 API token
- AIRTABLE_API_KEY: Airtable Personal Access Token
- AIRTABLE_BASE_ID, AIRTABLE_TABLE, AIRTABLE_VIEW, AIRTABLE_DOMAIN_FIELD, AIRTABLE_GEO_FIELD
- BLOCKLIST_BASE_IDS: comma-separated Airtable Base IDs of blocklist tables
- BLOCKLIST_TABLE_NAME: table name for blocklists (if same across)
- BLOCKLIST_DOMAIN_FIELD: domain field name in blocklists

Run locally:
    pip install -r requirements.txt
    streamlit run app.py

Notes:
- Ahrefs v3 exact parameter names can vary by plan; tweak the endpoint/filters in `AhrefsClient` if needed.
- For Gambling.com, we fetch REFERRING DOMAINS (full) once and cache.
"""

import os
import time
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Set, Tuple

import requests
import pandas as pd
import streamlit as st
import tldextract
from dateutil.parser import isoparse
from pydantic import BaseModel, Field, validator
from urllib.parse import urlparse

# ----------------------------
# Utilities
# ----------------------------

def extract_registrable_domain(url_or_host: str) -> str:
    if not url_or_host:
        return ""
    ext = tldextract.extract(url_or_host)
    if not ext.suffix:
        return (ext.domain or "").lower()
    return f"{ext.domain}.{ext.suffix}".lower()

def url_to_host(url: str) -> str:
    try:
        p = urlparse(url)
        if p.netloc:
            return p.netloc
        return url
    except Exception:
        return url

# ----------------------------
# Config
# ----------------------------
class AppConfig(BaseModel):
    airtable_base_id: str
    airtable_table: str
    airtable_view: Optional[str] = None
    airtable_domain_field: str = Field(default="Domain")
    airtable_geo_field: Optional[str] = Field(default="Sheet Name (Geo)")

    blocklist_base_ids: Optional[str] = None
    blocklist_table: Optional[str] = None
    blocklist_domain_field: Optional[str] = None

    days: int = Field(default=14, ge=1, le=60)
    gambling_domain: str = Field(default="gambling.com")
    ahrefs_max_concurrent: int = Field(default=4, ge=1, le=10)
    ahrefs_sleep_between: float = Field(default=0.4)

    @validator("gambling_domain", pre=True)
    def norm_gdc(cls, v):
        return extract_registrable_domain(v)

# ----------------------------
# Airtable client
# ----------------------------
class AirtableClient:
    def __init__(self, api_key: str, base_id: str):
        self.api_key = api_key
        self.base_id = base_id
        self.base_url = f"https://api.airtable.com/v0/{base_id}"
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        })

    def fetch_domains(self, table: str, domain_field: str, view: Optional[str] = None, max_records: int = 5000) -> List[str]:
        url = f"{self.base_url}/{requests.utils.quote(table)}"
        params = {"pageSize": 100}
        if view:
            params["view"] = view
        rows: List[str] = []
        offset = None
        while True:
            if offset:
                params["offset"] = offset
            r = self.session.get(url, params=params, timeout=60)
            r.raise_for_status()
            data = r.json()
            for rec in data.get("records", []):
                fields = rec.get("fields", {})
                raw_val = fields.get(domain_field)
                if not raw_val:
                    continue
                values = raw_val if isinstance(raw_val, list) else [raw_val]
                for v in values:
                    host = url_to_host(str(v))
                    reg = extract_registrable_domain(host)
                    if reg:
                        rows.append(reg)
            offset = data.get("offset")
            if not offset or len(rows) >= max_records:
                break
        return sorted(set(rows))

# ----------------------------
# Ahrefs client
# ----------------------------
class AhrefsClient:
    BASE = "https://api.ahrefs.com/v3"
    ENDPOINT_BACKLINKS = "/site-explorer/backlinks"
    ENDPOINT_REF_DOMAINS = "/site-explorer/referring-domains"

    def __init__(self, token: str):
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        })

    def _get_paginated(self, path: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        url = f"{self.BASE}{path}"
        out: List[Dict[str, Any]] = []
        cursor = None
        while True:
            q = params.copy()
            if cursor:
                q["cursor"] = cursor
            r = self.session.get(url, params=q, timeout=120)
            if r.status_code == 429:
                time.sleep(2)
                continue
            r.raise_for_status()
            data = r.json()
            items = data.get("data") or data.get("items") or []
            rows = items.get("rows", []) if isinstance(items, dict) else items
            out.extend(rows)
            cursor = (data.get("next") or data.get("cursor"))
            if not cursor:
                break
        return out

    def fetch_new_backlinks_last_n_days(self, target_domain: str, days: int = 14, mode: str = "domain") -> List[Dict[str, Any]]:
        since_date = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()
        params = {
            "target": target_domain,
            "mode": mode,
            "order_by": "first_seen:desc",
            "limit": 1000,
            "where": f"first_seen:gte:{since_date}",
        }
        rows = self._get_paginated(self.ENDPOINT_BACKLINKS, params)
        filtered = []
        for row in rows:
            src_url = row.get("url_from") or row.get("source_url") or row.get("page")
            first_seen = row.get("first_seen") or row.get("first_seen_date") or row.get("date")
            if not src_url:
                continue
            if first_seen:
                try:
                    d = isoparse(first_seen).date()
                except Exception:
                    try:
                        d = datetime.strptime(first_seen[:10], "%Y-%m-%d").date()
                    except Exception:
                        d = None
                if d and d < datetime.fromisoformat(since_date).date():
                    continue
            filtered.append({
                "source_url": src_url,
                "first_seen": first_seen,
                "target": target_domain,
                "raw": row,
            })
        return filtered

    def fetch_referring_domains(self, target_domain: str, mode: str = "domain") -> List[str]:
        params = {
            "target": target_domain,
            "mode": mode,
            "order_by": "dr:desc",
            "limit": 1000,
        }
        rows = self._get_paginated(self.ENDPOINT_REF_DOMAINS, params)
        domains: List[str] = []
        for r in rows:
            host = r.get("referring_domain") or r.get("domain") or r.get("host")
            if not host:
                url_from = r.get("url_from") or r.get("source_url")
                host = url_to_host(url_from) if url_from else None
            if host:
                domains.append(extract_registrable_domain(host))
        return sorted({d for d in domains if d})

# ----------------------------
# Cache
# ----------------------------
DB_PATH = "./backlink_cache.sqlite"
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ref_domain_cache (
  target_domain TEXT NOT NULL,
  fetched_at TEXT NOT NULL,
  domains_json TEXT NOT NULL,
  PRIMARY KEY (target_domain)
);
"""

def ensure_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(CREATE_TABLE_SQL)
        conn.commit()

def save_ref_domains_to_cache(target_domain: str, domains: List[str]):
    ensure_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "REPLACE INTO ref_domain_cache (target_domain, fetched_at, domains_json) VALUES (?, ?, ?)",
            (target_domain, datetime.utcnow().isoformat(), json.dumps(domains)),
        )
        conn.commit()

def load_ref_domains_from_cache(target_domain: str) -> Optional[Tuple[datetime, List[str]]]:
    ensure_db()
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            "SELECT fetched_at, domains_json FROM ref_domain_cache WHERE target_domain = ?",
            (target_domain,)
        )
        row = cur.fetchone()
        if not row:
            return None
        fetched_at = datetime.fromisoformat(row[0])
        domains = json.loads(row[1])
        return fetched_at, domains

# ----------------------------
# Streamlit UI
# ----------------------------
st.set_page_config(page_title="Competitor New Backlinks vs Gambling.com", layout="wide")

S = st.secrets if hasattr(st, "secrets") else {}

DEFAULT_BASE = S.get("AIRTABLE_BASE_ID", "appDEgCV6C4vLGjEY")
DEFAULT_TABLE = S.get("AIRTABLE_TABLE", "Sheet1")
DEFAULT_VIEW = S.get("AIRTABLE_VIEW", "")
DEFAULT_DOMAIN_FIELD = S.get("AIRTABLE_DOMAIN_FIELD", "Domain")
DEFAULT_GEO_FIELD = S.get("AIRTABLE_GEO_FIELD", "Sheet Name (Geo)")
DEFAULT_GAMBLING = S.get("GAMBLING_DOMAIN", "gambling.com")
BLOCKLIST_IDS = S.get("BLOCKLIST_BASE_IDS", "").split(",") if S.get("BLOCKLIST_BASE_IDS") else []
BLOCKLIST_TABLE = S.get("BLOCKLIST_TABLE_NAME", "Domains")
BLOCKLIST_FIELD = S.get("BLOCKLIST_DOMAIN_FIELD", "Domain")

with st.sidebar:
    st.header("Configuration")
    airtable_base_id = st.text_input("Airtable Base ID", value=DEFAULT_BASE)
    airtable_table = st.text_input("Airtable Table Name", value=DEFAULT_TABLE)
    airtable_view = st.text_input("Airtable View (optional)", value=DEFAULT_VIEW)
    airtable_domain_field = st.text_input("Airtable Domain Field", value=DEFAULT_DOMAIN_FIELD)
    airtable_geo_field = st.text_input("Airtable Geo Field", value=DEFAULT_GEO_FIELD)
    days = st.number_input("Window (days)", min_value=1, max_value=60, value=14)
    gambling_domain = st.text_input("Gambling.com domain", value=DEFAULT_GAMBLING)
    run_btn = st.button("Run comparison", type="primary")
    refresh_cache = st.button("Refresh Gambling.com cache")

AHREFS_TOKEN = os.getenv("AHREFS_API_TOKEN", S.get("AHREFS_API_TOKEN", ""))
AIRTABLE_TOKEN = os.getenv("AIRTABLE_API_KEY", S.get("AIRTABLE_API_KEY", ""))
if not AHREFS_TOKEN:
    st.info("Set AHREFS_API_TOKEN in Streamlit Secrets or environment.")
if not AIRTABLE_TOKEN:
    st.info("Set AIRTABLE_API_KEY in Streamlit Secrets or environment.")

def run_pipeline(force_refresh_cache: bool = False):
    st.write("### 1) Fetch competitors from primary Airtable…")
    at = AirtableClient(AIRTABLE_TOKEN, airtable_base_id)
    competitors = at.fetch_domains(airtable_table, airtable_domain_field, view=airtable_view)
    st.write(f"Found {len(competitors)} competitor domains.")

    # Load blocklist
    st.write("### 2) Loading blocklist Airtable bases…")
    blocklist_domains: Set[str] = set()
    for base in BLOCKLIST_IDS:
        if base.strip():
            atb = AirtableClient(AIRTABLE_TOKEN, base.strip())
            blocklist_domains.update(atb.fetch_domains(BLOCKLIST_TABLE, BLOCKLIST_FIELD))
    st.write(f"Loaded {len(blocklist_domains)} blocklisted domains from {len(BLOCKLIST_IDS)} Airtable bases.")

    # Load/refresh Gambling.com cache
    st.write("### 3) Gambling.com referring domains")
    ah = AhrefsClient(AHREFS_TOKEN)
    cache_hit = None if force_refresh_cache else load_ref_domains_from_cache(gambling_domain)
    if cache_hit:
        fetched_at, gdc_ref_domains = cache_hit
        st.write(f"Cache hit: {len(gdc_ref_domains)} domains (cached {fetched_at.isoformat()})")
    else:
        with st.spinner("Fetching referring domains from Ahrefs…"):
            gdc_ref_domains = ah.fetch_referring_domains(gambling_domain)
            save_ref_domains_to_cache(gambling_domain, gdc_ref_domains)
        st.write(f"Fetched and cached {len(gdc_ref_domains)} referring domains for {gambling_domain}.")

    gdc_ref_set: Set[str] = set(gdc_ref_domains)

    # Fetch Ahrefs new backlinks
    st.write("### 4) Fetching new backlinks from Ahrefs…")
    output_records: List[Dict[str, Any]] = []
    total = len(competitors)
    progress = st.progress(0.0)
    for i, comp in enumerate(competitors, 1):
        try:
            rows = ah.fetch_new_backlinks_last_n_days(comp, days=days)
        except requests.HTTPError as e:
            st.error(f"Ahrefs error for {comp}: {e}")
            rows = []
        for r in rows:
            src = r.get("source_url")
            reg = extract_registrable_domain(url_to_host(src))
            if not reg:
                continue
            if reg not in gdc_ref_set and reg not in blocklist_domains:
                output_records.append({
                    "competitor": comp,
                    "linking_domain": reg,
                    "source_url": src,
                    "first_seen": r.get("first_seen"),
                })
        progress.progress(i/total if total else 1.0)
        time.sleep(0.4)

    if not output_records:
        st.success("No exclusive domains found after filtering Gambling.com and blocklists.")
        return

    df = pd.DataFrame.from_records(output_records)
    df = df.drop_duplicates(subset=["competitor", "linking_domain"])
    st.write("### 5) Final results — exclusive domains")
    st.dataframe(df, use_container_width=True)
    st.download_button("Download CSV", df.to_csv(index=False).encode("utf-8"), "exclusive_domains.csv")

if run_btn:
    if not (airtable_base_id and airtable_table and AHREFS_TOKEN and AIRTABLE_TOKEN):
        st.error("Please provide Airtable Base/Table and set AHREFS_API_TOKEN and AIRTABLE_API_KEY in Secrets or environment.")
    else:
        run_pipeline(force_refresh_cache=False)

if refresh_cache:
    if not AHREFS_TOKEN:
        st.error("AHREFS_API_TOKEN missing.")
    else:
        run_pipeline(force_refresh_cache=True)

st.caption("Includes blocklist check across additional Airtable bases. Any linking domains appearing there will be excluded from the final output.")
