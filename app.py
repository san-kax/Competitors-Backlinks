"""
Streamlit app: Airtable → Ahrefs (last N days backlinks) vs Gambling.com referring domains

Secrets (Streamlit → Settings → Secrets) or env:
- AHREFS_API_TOKEN
- AIRTABLE_API_KEY
- AIRTABLE_BASE_ID, AIRTABLE_TABLE, AIRTABLE_VIEW (optional), AIRTABLE_DOMAIN_FIELD
- BLOCKLIST_BASE_IDS (comma-separated), BLOCKLIST_TABLE_NAME, BLOCKLIST_VIEW_ID (optional), BLOCKLIST_DOMAIN_FIELD
- GAMBLING_DOMAIN (default "gambling.com")
"""

import os
import time
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Set, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import pandas as pd
import streamlit as st
import tldextract
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
        return p.netloc or url
    except Exception:
        return url

def iso_window_last_n_days(days: int) -> tuple[str, str]:
    end = datetime.now(timezone.utc).replace(microsecond=0)
    start = (end - timedelta(days=days)).replace(hour=0, minute=0, second=0)
    return (
        start.isoformat().replace("+00:00", "Z"),
        end.isoformat().replace("+00:00", "Z"),
    )

def sanitize_target_for_ahrefs(val: str) -> Optional[str]:
    """Return a clean host for Ahrefs (no scheme/path/spaces); None if invalid."""
    if not val:
        return None
    s = str(val).strip().lower()
    try:
        p = urlparse(s)
        if p.scheme or p.netloc:
            s = p.netloc
    except Exception:
        pass
    s = " ".join(s.split()).strip(" /.")
    if not s or " " in s or "." not in s or len(s) > 255:
        return None
    try:
        s = s.encode("idna").decode("ascii")
    except Exception:
        return None
    s = s.rstrip("./")
    reg = extract_registrable_domain(s)
    return s if reg else None

def merge_unique(*lists: List[str]) -> List[str]:
    s: Set[str] = set()
    for lst in lists:
        if lst:
            s.update(lst)
    return sorted(s)

# ----------------------------
# HTTP session factory (faster & robust)
# ----------------------------

def make_session() -> requests.Session:
    sess = requests.Session()
    # retries & backoff for transient errors/rate limits
    retries = Retry(
        total=5,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=50, pool_maxsize=50)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    sess.headers.update({
        "Accept-Encoding": "gzip, deflate",
        "User-Agent": "gdc-competitor-backlinks/1.0",
        "Connection": "keep-alive",
    })
    return sess

# ----------------------------
# Airtable client
# ----------------------------

class AirtableClient:
    def __init__(self, api_key: str, base_id: str, session: Optional[requests.Session] = None):
        self.base_url = f"https://api.airtable.com/v0/{base_id}"
        self.session = session or make_session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
        })

    def fetch_domains(
        self,
        table_or_id: str,
        domain_field: str,
        view_or_id: Optional[str] = None,
        max_records: int = 20000,
    ) -> List[str]:
        url = f"{self.base_url}/{requests.utils.quote(table_or_id)}"
        params = {
            "pageSize": 100,
            "maxRecords": max_records,
        }
        # request only the domain field to reduce payload
        params["fields[]"] = domain_field
        if view_or_id:
            params["view"] = view_or_id

        rows: List[str] = []
        offset = None
        while True:
            if offset:
                params["offset"] = offset
            r = self.session.get(url, params=params, timeout=40)
            if not r.ok:
                raise requests.HTTPError(
                    f"Airtable error {r.status_code} for table '{table_or_id}': {r.text[:300]}"
                )
            data = r.json()
            for rec in data.get("records", []):
                fields = rec.get("fields", {})
                raw = fields.get(domain_field)
                if not raw:
                    continue
                values = raw if isinstance(raw, list) else [raw]
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
    ENDPOINT_BACKLINKS = "/site-explorer/all-backlinks"

    def __init__(self, token: str, session: Optional[requests.Session] = None):
        self.session = session or make_session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        })

    def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.BASE}{path}"
        r = self.session.get(url, params=params, timeout=60)
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

    # Baseline: all-time referring domains via /all-backlinks (1_per_domain), with required select
    def fetch_referring_domains_alltime(self, target_domain: str, mode: str = "domain") -> List[str]:
        rows = self._paginate(self.ENDPOINT_BACKLINKS, {
            "target": target_domain,
            "mode": mode,                # we'll call both domain & subdomains in pipeline
            "limit": 1000,
            "history": "all_time",
            "aggregation": "1_per_domain",
            "select": "root_name_source,url_from,root_domain,referring_domain,host",
        })
        out = []
        for r in rows:
            cand = (
                r.get("root_name_source") or
                r.get("root_domain") or
                r.get("referring_domain") or
                r.get("host")
            )
            if not cand:
                uf = r.get("url_from")
                if uf:
                    cand = url_to_host(uf)
            if cand:
                reg = extract_registrable_domain(cand)
                if reg:
                    out.append(reg)
        return sorted({d for d in out if d})

    # New backlinks (last N days), de-duped by domain, only live links
    def fetch_new_backlinks_last_n_days(self, target_domain: str, days: int = 14, mode: str = "domain") -> List[Dict[str, Any]]:
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
            "mode": mode,
            "limit": 1000,
            "history": f"since:{start_iso[:10]}",
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
# Local cache (SQLite)
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
    if not domains:   # don't persist empty results
        return
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

def clear_cache_row(target_domain: str):
    ensure_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM ref_domain_cache WHERE target_domain = ?", (target_domain,))
        conn.commit()

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

# Other defaults
DEFAULT_GAMBLING = S.get("GAMBLING_DOMAIN", "gambling.com")

with st.sidebar:
    st.header("Configuration")
    airtable_base_id = st.text_input("Airtable Base ID", value=DEFAULT_BASE)
    airtable_table   = st.text_input("Airtable Table (name or ID)", value=DEFAULT_TABLE)
    airtable_view    = st.text_input("Airtable View (optional name or ID)", value=DEFAULT_VIEW)
    airtable_domain_field = st.text_input("Airtable Domain Field", value=DEFAULT_DOMAIN_FIELD)
    days = st.number_input("Window (days)", min_value=1, max_value=60, value=14)
    gambling_domain = st.text_input("Gambling.com domain", value=DEFAULT_GAMBLING)
    max_concurrency = st.slider("Ahrefs concurrency", 2, 20, 8, help="Parallel competitor calls")
    show_debug = st.checkbox("Show debug counts", value=False)
    run_btn = st.button("Run comparison", type="primary")
    refresh_cache = st.button("Refresh Gambling.com cache")
    clear_cache_btn = st.button("Clear Gambling.com cache")

AHREFS_TOKEN   = os.getenv("AHREFS_API_TOKEN", S.get("AHREFS_API_TOKEN", ""))
AIRTABLE_TOKEN = os.getenv("AIRTABLE_API_KEY",   S.get("AIRTABLE_API_KEY",   ""))

if not AHREFS_TOKEN:
    st.info("Set AHREFS_API_TOKEN in Streamlit Secrets or environment.")
if not AIRTABLE_TOKEN:
    st.info("Set AIRTABLE_API_KEY in Streamlit Secrets or environment.")

if clear_cache_btn:
    clear_cache_row(gambling_domain)
    st.success("Cleared cache entry for Gambling.com. Click 'Refresh Gambling.com cache' to fetch again.")

# Shared HTTP session for speed
shared_session = make_session()

# ----------------------------
# Pipeline
# ----------------------------

def run_pipeline(force_refresh_cache: bool = False):
    # 1) Competitors
    st.write("## 1) Fetch competitors from primary Airtable…")
    at = AirtableClient(AIRTABLE_TOKEN, airtable_base_id, session=shared_session)
    competitors_raw = at.fetch_domains(airtable_table, airtable_domain_field, view_or_id=(airtable_view or None))
    # sanitize and de-dupe
    competitors = []
    for d in competitors_raw:
        t = sanitize_target_for_ahrefs(d)
        if t:
            competitors.append(t)
    competitors = sorted(set(competitors))
    st.write(f"Found **{len(competitors)}** competitor domains.")

    # 2) Blocklists (threaded)
    st.write("## 2) Loading blocklist Airtable bases…")
    blocklist_domains: Set[str] = set()
    load_errors = []

    def load_blocklist(base_id: str) -> Set[str]:
        if not base_id.strip():
            return set()
        cli = AirtableClient(AIRTABLE_TOKEN, base_id.strip(), session=shared_session)
        try:
            lst = cli.fetch_domains(BLOCKLIST_TABLE, BLOCKLIST_FIELD, view_or_id=(BLOCKLIST_VIEW_ID or None))
            return set(lst)
        except requests.HTTPError as e:
            load_errors.append(str(e))
            return set()

    if BLOCKLIST_IDS:
        with ThreadPoolExecutor(max_workers=min(8, len(BLOCKLIST_IDS))) as pool:
            futures = [pool.submit(load_blocklist, b) for b in BLOCKLIST_IDS]
            for fut in as_completed(futures):
                blocklist_domains.update(fut.result() or set())

    st.write(f"Loaded **{len(blocklist_domains)}** blocklisted domains from **{len([b for b in BLOCKLIST_IDS if b.strip()])}** base(s).")
    if load_errors:
        with st.expander("Blocklist load warnings"):
            for err in load_errors:
                st.write(f"- {err}")

    # 3) Gambling.com baseline (cache + fresh if needed)
    st.write("## 3) Gambling.com referring domains (baseline)")
    ah = AhrefsClient(AHREFS_TOKEN, session=shared_session)

    cache_hit = None if force_refresh_cache else load_ref_domains_from_cache(gambling_domain)
    if cache_hit:
        fetched_at, gdc_ref_domains = cache_hit
        st.write(f"Cache hit: **{len(gdc_ref_domains)}** domains (cached {fetched_at.isoformat()})")
    else:
        with st.spinner("Fetching baseline ref domains (all_time, 1_per_domain)…"):
            t_naked = sanitize_target_for_ahrefs(gambling_domain) or "gambling.com"
            t_www   = "www.gambling.com" if "www." not in t_naked else t_naked

            parts, errors = [], []

            def fetch_one(tgt: str, md: str) -> List[str]:
                try:
                    r = ah.fetch_referring_domains_alltime(tgt, mode=md)
                    if show_debug:
                        st.write(f"• {tgt} [{md}] → {len(r)} domains")
                    return r
                except requests.HTTPError as e:
                    errors.append(f"{tgt} [{md}]: {e}")
                    return []

            # Try all four in parallel for speed
            combos = [(t_naked, "domain"), (t_naked, "subdomains"), (t_www, "domain"), (t_www, "subdomains")]
            with ThreadPoolExecutor(max_workers=4) as pool:
                futures = [pool.submit(fetch_one, t, m) for (t, m) in combos]
                for fut in as_completed(futures):
                    parts.append(fut.result())

            gdc_ref_domains = merge_unique(*parts)
            if errors:
                with st.expander("Baseline fetch warnings/errors"):
                    for e in errors:
                        st.write("- " + e)

            if gdc_ref_domains:
                save_ref_domains_to_cache(gambling_domain, gdc_ref_domains)

        st.write(f"Fetched {'**0**' if not gdc_ref_domains else f'**{len(gdc_ref_domains)}**'} referring domains for **{gambling_domain}**.")
    gdc_ref_set: Set[str] = set(gdc_ref_domains)

    # 4) New backlinks per competitor (threaded)
    st.write("## 4) Fetching new backlinks from Ahrefs (last N days)…")
    output_records: List[Dict[str, Any]] = []
    bad_targets = 0

    def fetch_comp(comp: str) -> List[Dict[str, Any]]:
        try:
            rows = ah.fetch_new_backlinks_last_n_days(comp, days=days, mode="domain")
            if show_debug:
                return [{"_debug": f"{comp}: {len(rows)} new"}, *rows]
            return rows
        except requests.HTTPError as e:
            return [{"_error": f"{comp}: {e}"}]

    if competitors:
        prog = st.progress(0.0)
        done = 0
        with ThreadPoolExecutor(max_workers=max_concurrency) as pool:
            futures = [pool.submit(fetch_comp, comp) for comp in competitors]
            for fut in as_completed(futures):
                res = fut.result() or []
                # handle debug/error records
                keep = []
                for r in res:
                    if "_debug" in r:
                        if show_debug:
                            st.write(r["_debug"])
                    elif "_error" in r:
                        st.error(r["_error"])
                    else:
                        keep.append(r)
                for r in keep:
                    src = r.get("source_url")
                    reg = extract_registrable_domain(url_to_host(src))
                    if not reg:
                        continue
                    if reg not in gdc_ref_set and reg not in blocklist_domains:
                        output_records.append({
                            "linking_domain": reg,
                            "source_url": src,
                            "first_seen": r.get("first_seen"),
                        })
                done += 1
                prog.progress(done / len(competitors))

    # 5) Results
    st.write("## 5) Final results — exclusive domains")
    if not output_records:
        st.success("No exclusive domains found after filtering Gambling.com baseline and blocklists.")
        return
    df = pd.DataFrame.from_records(output_records).drop_duplicates(subset=["linking_domain"])
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

st.caption("Baseline via Ahrefs v3 /site-explorer/all-backlinks (history=all_time, aggregation=1_per_domain, select=root_name_source,url_from,...). New backlinks via same endpoint with first_seen window + last_seen is null. Threaded fetch & pooled HTTP for speed. Targets sanitized; empty results are not cached.")
