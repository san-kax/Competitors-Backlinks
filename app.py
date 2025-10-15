"""
Streamlit app: Airtable → Ahrefs (last N days backlinks) vs Gambling.com referring domains

Secrets required (Streamlit → Settings → Secrets) or env vars:
- AHREFS_API_TOKEN
- AIRTABLE_API_KEY
- AIRTABLE_BASE_ID, AIRTABLE_TABLE, AIRTABLE_VIEW (optional), AIRTABLE_DOMAIN_FIELD
- BLOCKLIST_BASE_IDS (comma-separated), BLOCKLIST_TABLE_NAME (table ID or name), BLOCKLIST_VIEW_ID (optional), BLOCKLIST_DOMAIN_FIELD
- GAMBLING_DOMAIN (default "gambling.com")
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
from urllib.parse import urlparse

# ----------------------------
# Small utils
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

def iso_window_last_n_days(days: int) -> tuple[str, str]:
    # Inclusive UTC window; end is "now"
    end = datetime.now(timezone.utc).replace(microsecond=0)
    start = (end - timedelta(days=days)).replace(hour=0, minute=0, second=0)
    return (
        start.isoformat().replace("+00:00", "Z"),
        end.isoformat().replace("+00:00", "Z"),
    )

def sanitize_target_for_ahrefs(val: str) -> Optional[str]:
    """Return a clean host (no scheme, no path, no spaces) or None if invalid."""
    if not val:
        return None
    s = str(val).strip().lower()
    # parse if full URL
    try:
        p = urlparse(s)
        if p.scheme or p.netloc:
            s = p.netloc
    except Exception:
        pass
    # remove trailing/leading junk and collapse spaces
    s = " ".join(s.split()).strip(" /.")
    if not s or " " in s or "." not in s or len(s) > 255:
        return None
    # idna-safe
    try:
        s = s.encode("idna").decode("ascii")
    except Exception:
        return None
    # remove trailing dot or slash if any slipped in
    s = s.rstrip("./")
    # keep subdomain if present but make sure registrable part exists
    reg = extract_registrable_domain(s)
    return s if reg else None

# ----------------------------
# Airtable client
# ----------------------------

class AirtableClient:
    def __init__(self, api_key: str, base_id: str):
        self.base_url = f"https://api.airtable.com/v0/{base_id}"
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        })

    def fetch_domains(
        self,
        table_or_id: str,
        domain_field: str,
        view_or_id: Optional[str] = None,
        max_records: int = 10000,
    ) -> List[str]:
        url = f"{self.base_url}/{requests.utils.quote(table_or_id)}"
        params = {"pageSize": 100}
        if view_or_id:
            params["view"] = view_or_id

        rows: List[str] = []
        offset = None
        while True:
            if offset:
                params["offset"] = offset
            r = self.session.get(url, params=params, timeout=60)
            if not r.ok:
                raise requests.HTTPError(
                    f"Airtable error {r.status_code} for table '{table_or_id}': {r.text[:300]}"
                )
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
# Ahrefs client (v3)
# ----------------------------

class AhrefsClient:
    BASE = "https://api.ahrefs.com/v3"
    ENDPOINT_BACKLINKS = "/site-explorer/all-backlinks"   # new backlinks + fallback
    ENDPOINT_REF_DOMAINS = "/site-explorer/refdomains"    # primary for ref domains

    def __init__(self, token: str):
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        })

    def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.BASE}{path}"
        r = self.session.get(url, params=params, timeout=120)
        if r.status_code == 429:
            time.sleep(2)
            r = self.session.get(url, params=params, timeout=120)
        if not r.ok:
            raise requests.HTTPError(f"Ahrefs v3 {path} failed: HTTP {r.status_code} :: {r.text[:300]}")
        return r.json()

    def _paginate(self, path: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        cursor = None
        while True:
            q = params.copy()
            if cursor:
                q["cursor"] = cursor
            data = self._get(path, q)
            items = data.get("data") or data.get("items") or {}
            rows = items.get("rows", []) if isinstance(items, dict) else items
            out.extend(rows)
            cursor = data.get("next") or data.get("cursor")
            if not cursor:
                break
        return out

    def fetch_referring_domains(self, target_domain: str, mode: str = "domain") -> List[str]:
        # Primary: /refdomains with select + history=all_time
        try:
            rows = self._paginate(self.ENDPOINT_REF_DOMAINS, {
                "target": target_domain,
                "mode": mode,
                "limit": 1000,
                "select": "refdomain",
                "history": "all_time",
            })
            out = []
            for r in rows:
                host = (
                    r.get("refdomain") or
                    r.get("referring_domain") or
                    r.get("domain") or
                    r.get("host") or
                    r.get("url_from")
                )
                if host:
                    out.append(extract_registrable_domain(url_to_host(host)))
            uniq = sorted({d for d in out if d})
            if uniq:
                return uniq
        except requests.HTTPError:
            pass  # fall back below

        # Fallback: /all-backlinks aggregated to 1_per_domain (full-ish set)
        rows = self._paginate(self.ENDPOINT_BACKLINKS, {
            "target": target_domain,
            "mode": mode,
            "limit": 1000,
            "aggregation": "1_per_domain",
            "select": "root_name_source,url_from",
            "order_by": "url_rating_source:desc",
            "history": "all_time",
        })
        out = []
        for r in rows:
            host = r.get("root_name_source") or r.get("url_from")
            if host:
                out.append(extract_registrable_domain(url_to_host(host)))
        return sorted({d for d in out if d})

    def fetch_new_backlinks_last_n_days(self, target_domain: str, days: int = 14, mode: str = "domain") -> List[Dict[str, Any]]:
        # Match your working curl: use /all-backlinks with select + where window + last_seen is null + 1_per_domain
        start_iso, end_iso = iso_window_last_n_days(days)
        where_obj = {
            "and": [
                {"field": "first_seen_link", "is": ["gte", start_iso]},
                {"field": "first_seen_link", "is": ["lte", end_iso]},
                {"field": "last_seen", "is": "is_null"},
            ]
        }
        params = {
            "target": target_domain,
            "mode": mode,                      # "domain" or "subdomains"
            "limit": 1000,
            "history": f"since:{start_iso[:10]}",  # narrows historical window server-side
            "order_by": "traffic:desc,url_rating_source:desc",
            "aggregation": "1_per_domain",
            "protocol": "both",
            "select": "url_from,anchor,first_seen_link,traffic,url_rating_source",
            "where": json.dumps(where_obj),
        }
        rows = self._paginate(self.ENDPOINT_BACKLINKS, params)
        out = []
        for row in rows:
            src_url = row.get("url_from")
            first_seen = row.get("first_seen_link")
            if src_url:
                out.append({"source_url": src_url, "first_seen": first_seen, "raw": row})
        return out

# ----------------------------
# Local cache (SQLite) for ref domains
# ----------------------------

DB_PATH = "./backlink_cache.sqlite"
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ref_domain_cache (
  target_domain TEXT NOT NULL PRIMARY KEY,
  fetched_at TEXT NOT NULL,
  domains_json TEXT NOT NULL
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
        return datetime.fromisoformat(row[0]), json.loads(row[1])

# ----------------------------
# Streamlit UI
# ----------------------------

st.set_page_config(page_title="Competitor New Backlinks vs Gambling.com", layout="wide")
S = st.secrets if hasattr(st, "secrets") else {}

# Primary Airtable (competitors)
DEFAULT_BASE  = S.get("AIRTABLE_BASE_ID", "appDEgCV6C4vLGjEY")
DEFAULT_TABLE = S.get("AIRTABLE_TABLE", "Sheet1")
DEFAULT_VIEW  = S.get("AIRTABLE_VIEW", "")
DEFAULT_DOMAIN_FIELD = S.get("AIRTABLE_DOMAIN_FIELD", "Domain")

# Blocklists
BLOCKLIST_IDS     = S.get("BLOCKLIST_BASE_IDS", "").split(",") if S.get("BLOCKLIST_BASE_IDS") else []
BLOCKLIST_TABLE   = S.get("BLOCKLIST_TABLE_NAME", "tbliCOQZY9RICLsLP")
BLOCKLIST_VIEW_ID = S.get("BLOCKLIST_VIEW_ID", "")
BLOCKLIST_FIELD   = S.get("BLOCKLIST_DOMAIN_FIELD", "Domain")

DEFAULT_GAMBLING = S.get("GAMBLING_DOMAIN", "gambling.com")

with st.sidebar:
    st.header("Configuration")
    airtable_base_id = st.text_input("Airtable Base ID", value=DEFAULT_BASE)
    airtable_table   = st.text_input("Airtable Table (name or ID)", value=DEFAULT_TABLE)
    airtable_view    = st.text_input("Airtable View (optional name or ID)", value=DEFAULT_VIEW)
    airtable_domain_field = st.text_input("Airtable Domain Field", value=DEFAULT_DOMAIN_FIELD)
    days = st.number_input("Window (days)", min_value=1, max_value=60, value=14)
    gambling_domain = st.text_input("Gambling.com domain", value=DEFAULT_GAMBLING)
    run_btn = st.button("Run comparison", type="primary")
    refresh_cache = st.button("Refresh Gambling.com cache")

AHREFS_TOKEN   = os.getenv("AHREFS_API_TOKEN", S.get("AHREFS_API_TOKEN", ""))
AIRTABLE_TOKEN = os.getenv("AIRTABLE_API_KEY",   S.get("AIRTABLE_API_KEY",   ""))

if not AHREFS_TOKEN:
    st.info("Set AHREFS_API_TOKEN in Streamlit Secrets or environment.")
if not AIRTABLE_TOKEN:
    st.info("Set AIRTABLE_API_KEY in Streamlit Secrets or environment.")

# ----------------------------
# Pipeline
# ----------------------------

def run_pipeline(force_refresh_cache: bool = False):
    # 1) Competitors
    st.write("## 1) Fetch competitors from primary Airtable…")
    at = AirtableClient(AIRTABLE_TOKEN, airtable_base_id)
    competitors = at.fetch_domains(airtable_table, airtable_domain_field, view_or_id=(airtable_view or None))
    st.write(f"Found **{len(competitors)}** competitor domains.")

    # 2) Blocklists
    st.write("## 2) Loading blocklist Airtable bases…")
    blocklist_domains: Set[str] = set()
    load_errors = []
    for base in BLOCKLIST_IDS:
        base = base.strip()
        if not base:
            continue
        atb = AirtableClient(AIRTABLE_TOKEN, base)
        try:
            blocklist_domains.update(
                atb.fetch_domains(BLOCKLIST_TABLE, BLOCKLIST_FIELD, view_or_id=(BLOCKLIST_VIEW_ID or None))
            )
        except requests.HTTPError as e:
            load_errors.append(str(e))
    st.write(f"Loaded **{len(blocklist_domains)}** blocklisted domains from **{len(BLOCKLIST_IDS)}** base(s).")
    if load_errors:
        with st.expander("Blocklist load warnings"):
            for err in load_errors:
                st.write(f"- {err}")

    # 3) Gambling.com ref domains (cache)
    st.write("## 3) Gambling.com referring domains")
    ah = AhrefsClient(AHREFS_TOKEN)
    cache_hit = None if force_refresh_cache else load_ref_domains_from_cache(gambling_domain)
    if cache_hit:
        fetched_at, gdc_ref_domains = cache_hit
        st.write(f"Cache hit: **{len(gdc_ref_domains)}** domains (cached {fetched_at.isoformat()})")
    else:
        with st.spinner("Fetching referring domains from Ahrefs v3…"):
            # sanitize gambling.com target too
            gdc_target = sanitize_target_for_ahrefs(gambling_domain) or "gambling.com"
            gdc_ref_domains = ah.fetch_referring_domains(gdc_target)
            save_ref_domains_to_cache(gambling_domain, gdc_ref_domains)
        st.write(f"Fetched and cached **{len(gdc_ref_domains)}** referring domains for **{gambling_domain}**.")
    gdc_ref_set: Set[str] = set(gdc_ref_domains)

    # 4) New backlinks per competitor
    st.write("## 4) Fetching new backlinks from Ahrefs (last N days)…")
    output_records: List[Dict[str, Any]] = []
    total = len(competitors)
    progress = st.progress(0.0)
    bad_targets = 0

    for i, comp in enumerate(competitors, 1):
        target = sanitize_target_for_ahrefs(comp)
        if not target:
            bad_targets += 1
            progress.progress(i / total if total else 1.0)
            continue

        try:
            rows = ah.fetch_new_backlinks_last_n_days(target, days=days, mode="domain")
        except requests.HTTPError as e:
            st.error(f"Ahrefs v3 error for {target}: {e}")
            rows = []

        for r in rows:
            src = r.get("source_url")
            reg = extract_registrable_domain(url_to_host(src))
            if not reg:
                continue
            if reg not in gdc_ref_set and reg not in blocklist_domains:
                output_records.append({
                    "competitor": target,
                    "linking_domain": reg,
                    "source_url": src,
                    "first_seen": r.get("first_seen"),
                })
        progress.progress(i / total if total else 1.0)
        time.sleep(0.15)

    if bad_targets:
        st.warning(f"Skipped {bad_targets} invalid targets from Airtable (values with spaces, full URLs, or typos).")

    # 5) Results
    st.write("## 5) Final results — exclusive domains")
    if not output_records:
        st.success("No exclusive domains found after filtering Gambling.com and blocklists.")
        return
    df = pd.DataFrame.from_records(output_records).drop_duplicates(subset=["competitor", "linking_domain"])
    st.dataframe(df, use_container_width=True)
    st.download_button("Download CSV", df.to_csv(index=False).encode("utf-8"), "exclusive_domains.csv")

# Buttons
if run_btn:
    if not (airtable_base_id and airtable_table and AHREFS_TOKEN and AIRTABLE_TOKEN):
        st.error("Please set AIRTABLE_* and AHREFS_API_TOKEN in Secrets or environment.")
    else:
        run_pipeline(force_refresh_cache=False)

if refresh_cache:
    if not AHREFS_TOKEN:
        st.error("AHREFS_API_TOKEN missing.")
    else:
        run_pipeline(force_refresh_cache=True)

st.caption("Ahrefs v3: /site-explorer/refdomains (history=all_time, select=refdomain) and /site-explorer/all-backlinks (select + where on first_seen_link; last_seen is null; 1_per_domain). Targets sanitized to avoid 'bad target' errors.")
