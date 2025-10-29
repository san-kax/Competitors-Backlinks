# v1.5.6 — Fixed success message to show actual count instead of API limit

import os, json, sqlite3
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

# ---------------- utils ----------------
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

def make_session() -> requests.Session:
    sess = requests.Session()
    retries = Retry(
        total=5, backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=["GET"], raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=80, pool_maxsize=80)
    sess.mount("https://", adapter); sess.mount("http://", adapter)
    sess.headers.update({
        "Accept-Encoding":"gzip, deflate",
        "User-Agent":"gdc-competitor-backlinks/1.5.6",
        "Connection":"keep-alive",
    })
    return sess

# ---------------- Airtable ----------------
class AirtableClient:
    def __init__(self, api_key: str, base_id: str, session: Optional[requests.Session] = None):
        self.base_url = f"https://api.airtable.com/v0/{base_id}"
        self.session = session or make_session()
        self.session.headers.update({"Authorization": f"Bearer {api_key}"})

    def fetch_domains(self, table_or_id: str, domain_field: str, view_or_id: Optional[str] = None, max_records: int = 20000) -> List[str]:
        url = f"{self.base_url}/{requests.utils.quote(table_or_id)}"
        params = {"pageSize": 100, "maxRecords": max_records, "fields[]": domain_field}
        if view_or_id: params["view"] = view_or_id
        rows, offset = [], None
        while True:
            if offset: params["offset"] = offset
            r = self.session.get(url, params=params, timeout=40); r.raise_for_status()
            data = r.json()
            for rec in data.get("records", []):
                v = rec.get("fields", {}).get(domain_field)
                if not v: continue
                for x in (v if isinstance(v, list) else [v]):
                    host = url_to_host(str(x)); reg = extract_registrable_domain(host)
                    if reg: rows.append(reg)
            offset = data.get("offset")
            if not offset or len(rows) >= max_records: break
        return sorted(set(rows))

# ---------------- Ahrefs v3 ----------------
class AhrefsClient:
    BASE = "https://api.ahrefs.com/v3"
    EP_BACKLINKS = "/site-explorer/all-backlinks"
    EP_REFDOMAINS = "/site-explorer/refdomains"

    def __init__(self, token: str, session: Optional[requests.Session] = None):
        self.session = session or make_session()
        self.session.headers.update({"Authorization": f"Bearer {token}", "Content-Type": "application/json"})

    def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        r = self.session.get(f"{self.BASE}{path}", params=params, timeout=60)
        if not r.ok:
            raise requests.HTTPError(f"Ahrefs v3 {path} failed: HTTP {r.status_code} :: {r.text[:300]}")
        return r.json()

    def _paginate(self, path: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        out, cursor = [], None
        total_fetched = 0
        
        while True:
            q = params.copy()
            if cursor: q["cursor"] = cursor
            data = self._get(path, q)
            
            # FIXED: Check for backlinks array first (based on debug output)
            if "backlinks" in data:
                batch = data["backlinks"]
                out.extend(batch)
                total_fetched += len(batch)
            else:
                # Fallback to original structure
                items = data.get("data") or data.get("items") or {}
                rows = items.get("rows", []) if isinstance(items, dict) else items
                out.extend(rows)
                total_fetched += len(rows)
            
            # Show progress for large fetches
            if total_fetched > 1000 and total_fetched % 5000 == 0:
                st.info(f"📊 Progress: Fetched {total_fetched} backlinks so far...")
            
            cursor = data.get("next") or data.get("cursor")
            if not cursor: break
            
            # Safety check to prevent infinite loops
            if total_fetched > 100000:  # Reasonable upper limit
                st.warning(f"⚠️ Stopped at {total_fetched} backlinks to prevent timeout")
                break
                
        return out

    def test_exact_api_call(self, target: str) -> Dict[str, Any]:
        """Test the exact API call matching the Ahrefs interface"""
        
        # Ensure we use www.gambling.com format if it's gambling.com
        if target == "gambling.com":
            target = "www.gambling.com"
        
        # FIXED: Use higher limit to test the full API capability
        params = {
            "target": f"{target}/",  # www.gambling.com/
            "mode": "subdomains",
            "limit": 50000,  # FIXED: Increased from 100
            "history": "all_time", 
            "protocol": "both",
            "aggregation": "1_per_domain",  # Matches "One link per domain" in interface
            "order_by": "traffic:desc,url_rating_source:desc",
            "select": "url_from,link_group_count,title,languages,powered_by,link_type,redirect_code,first_seen_link,lost_reason,drop_reason,http_code,discovered_status,source_page_author,is_dofollow,is_nofollow,is_ugc,is_sponsored,is_content,domain_rating_source,traffic_domain,is_root_source,is_spam,root_name_source,traffic,positions,links_external,url_rating_source,last_visited,refdomains_source,linked_domains_source_page,snippet_left,anchor,snippet_right,url_to,js_crawl,http_crawl,redirect_kind,url_redirect,broken_redirect_source,broken_redirect_new_target,broken_redirect_reason,last_seen",
            "where": json.dumps({"field": "last_seen", "is": "is_null"})  # Live links only
        }
        
        try:
            result = self._get(self.EP_BACKLINKS, params)
            
            # Debug: Show the actual response structure
            if show_debug:
                st.write("🔍 API Response Debug:")
                st.json(result)
            
            # FIXED: Check the correct response structure (backlinks array)
            rows_count = 0
            if "backlinks" in result:
                rows_count = len(result["backlinks"])
            elif "data" in result and "rows" in result["data"]:
                rows_count = len(result["data"]["rows"])
            elif "items" in result:
                rows_count = len(result["items"])
            elif "rows" in result:
                rows_count = len(result["rows"])
            
            return {"success": True, "data": result, "rows_count": rows_count}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # ---- Updated baseline with exact working API format ----
    def baseline_all_backlinks_variants(self, target: str, mode: str) -> Tuple[List[str], str]:
        """Return (domains, variant_note)."""
        def rows_to_domains(rows: List[Dict[str, Any]]) -> List[str]:
            out=[]
            for r in rows:
                uf = r.get("url_from") or r.get("source_url") or r.get("page")
                if uf:
                    host = url_to_host(uf)
                    reg = extract_registrable_domain(host)
                    if reg: out.append(reg)
            return sorted(set(out))

        # Ensure we use www.gambling.com format if it's gambling.com
        if target == "gambling.com":
            target = "www.gambling.com"

        # FIXED: Use the exact working API call but with much higher limit
        exact_params = {
            "target": f"{target}/",  # www.gambling.com/
            "mode": "subdomains",    # Matches interface
            "limit": 50000,          # FIXED: Increased from 100 to handle 31k+ backlinks
            "history": "all_time",
            "protocol": "both",
            "aggregation": "1_per_domain",  # Matches "One link per domain"
            "order_by": "traffic:desc,url_rating_source:desc",
            "select": "url_from,link_group_count,title,languages,powered_by,link_type,redirect_code,first_seen_link,lost_reason,drop_reason,http_code,discovered_status,source_page_author,is_dofollow,is_nofollow,is_ugc,is_sponsored,is_content,domain_rating_source,traffic_domain,is_root_source,is_spam,root_name_source,traffic,positions,links_external,url_rating_source,last_visited,refdomains_source,linked_domains_source_page,snippet_left,anchor,snippet_right,url_to,js_crawl,http_crawl,redirect_kind,url_redirect,broken_redirect_source,broken_redirect_new_target,broken_redirect_reason,last_seen",
            "where": json.dumps({"field": "last_seen", "is": "is_null"})  # Live links only
        }
        
        try:
            st.info("🔄 Fetching ALL Gambling.com backlinks (this may take a moment for 31k+ results)...")
            rows = self._paginate(self.EP_BACKLINKS, exact_params)
            doms = rows_to_domains(rows)
            if doms:
                # FIXED: Show actual count instead of API limit
                st.success(f"✅ Successfully fetched {len(doms)} unique referring domains for Gambling.com baseline")
                return doms, f"/all-backlinks [exact Ahrefs interface format - {len(doms)} domains]"
        except requests.HTTPError as e:
            st.warning(f"Exact API format failed: {str(e)[:100]}...")

        # Fallback to simplified format with higher limit
        simple_params = {
            "target": f"{target}/",
            "mode": "subdomains",
            "limit": 50000,  # FIXED: Increased from 1000
            "history": "all_time",
            "protocol": "both",
            "aggregation": "1_per_domain",
            "where": json.dumps({"field": "last_seen", "is": "is_null"})
        }
        
        try:
            st.info("🔄 Trying simplified format for ALL Gambling.com backlinks...")
            rows = self._paginate(self.EP_BACKLINKS, simple_params)
            doms = rows_to_domains(rows)
            if doms:
                # FIXED: Show actual count instead of API limit
                st.success(f"✅ Successfully fetched {len(doms)} unique referring domains for Gambling.com baseline")
                return doms, f"/all-backlinks [simplified format - {len(doms)} domains]"
        except requests.HTTPError as e:
            st.warning(f"Simplified format failed: {str(e)[:100]}...")

        # Final fallback to original variants with higher limits
        def variant_params(history_all_time: bool, aggregation: bool, with_select: bool):
            p = {
                "target": f"{target}/",
                "mode": "subdomains",
                "limit": 50000,  # FIXED: Increased from 1000
                "history": "all_time" if history_all_time else "since:2000-01-01",
                "protocol": "both",
            }
            if aggregation: p["aggregation"] = "1_per_domain"
            if with_select: p["select"] = "url_from"
            return p

        variants = []
        for with_select in (True, False):
            for aggregation in (True, False):
                variants.append(("all_time", aggregation, with_select))

        last_err = None
        for hist, agg, sel in variants:
            history_all_time = (hist == "all_time")
            params = variant_params(history_all_time=history_all_time, aggregation=agg, with_select=sel)
            note = f"/all-backlinks [{hist}; agg={'on' if agg else 'off'}; select={'url_from' if sel else 'none'}]"
            try:
                st.info(f"🔄 Trying variant: {note}")
                rows = self._paginate(self.EP_BACKLINKS, params)
                doms = rows_to_domains(rows)
                if doms:
                    # FIXED: Show actual count instead of API limit
                    st.success(f"✅ Successfully fetched {len(doms)} unique referring domains for Gambling.com baseline")
                    return doms, f"{note} - {len(doms)} domains"
            except requests.HTTPError as e:
                last_err = e
                continue
        if last_err:
            return [], f"/all-backlinks failed ({str(last_err)[:90]}…)"

        return [], "/all-backlinks returned empty in all variants"

    def baseline_refdomains_relaxed(self, target: str, mode: str) -> Tuple[List[str], str]:
        # Ensure we use www.gambling.com format if it's gambling.com
        if target == "gambling.com":
            target = "www.gambling.com"
            
        params = { 
            "target": f"{target}/",
            "mode": "subdomains",
            "limit": 50000  # FIXED: Increased from 1000
        }
        try:
            rows = self._paginate(self.EP_REFDOMAINS, params)
        except requests.HTTPError as e:
            return [], f"/refdomains failed ({str(e)[:90]}…)"
        out=[]
        for r in rows:
            cand = r.get("domain") or r.get("referring_domain") or r.get("host")
            if cand:
                reg = extract_registrable_domain(cand)
                if reg: out.append(reg)
        return sorted(set(out)), f"/refdomains (no order_by/select - {len(sorted(set(out)))} domains)"

    # New backlinks (last N days) — keep minimal select
    def fetch_new_backlinks_last_n_days(self, target: str, days: int, mode: str = "domain") -> List[Dict[str, Any]]:
        start_iso, end_iso = iso_window_last_n_days(days)
        where_obj = {"and":[{"field":"first_seen_link","is":["gte",start_iso]},
                            {"field":"first_seen_link","is":["lte",end_iso]},
                            {"field":"last_seen","is":"is_null"}]}
        rows = self._paginate(self.EP_BACKLINKS, {
            "target": f"{target}/",
            "mode": "subdomains",
            "limit": 1000,
            "history": f"since:{start_iso[:10]}",
            "order_by": "traffic:desc,url_rating_source:desc",
            "aggregation": "1_per_domain", 
            "protocol":"both",
            "select": "url_from,first_seen_link",
            "where": json.dumps(where_obj),
        })
        out=[]
        for r in rows:
            uf = r.get("url_from"); fs = r.get("first_seen_link")
            if uf: out.append({"source_url": uf, "first_seen": fs, "raw": r})
        return out

# ---------------- cache ----------------
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
        conn.execute(CREATE_TABLE_SQL); conn.commit()

def save_ref_domains_to_cache(target_domain: str, domains: List[str]):
    if not domains: return
    ensure_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("REPLACE INTO ref_domain_cache (target_domain, fetched_at, domains_json) VALUES (?, ?, ?)",
                     (target_domain, datetime.utcnow().isoformat(), json.dumps(domains)))
        conn.commit()

def load_ref_domains_from_cache(target_domain: str) -> Optional[Tuple[datetime, List[str]]]:
    ensure_db()
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute("SELECT fetched_at, domains_json FROM ref_domain_cache WHERE target_domain = ?",
                           (target_domain,))
        row = cur.fetchone()
        return (datetime.fromisoformat(row[0]), json.loads(row[1])) if row else None

def clear_cache_row(target_domain: str):
    ensure_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM ref_domain_cache WHERE target_domain = ?", (target_domain,))
        conn.commit()

# ---------------- UI ----------------
st.set_page_config(page_title="Competitor New Backlinks vs Gambling.com", layout="wide")
S = st.secrets if hasattr(st, "secrets") else {}

DEFAULT_BASE  = S.get("AIRTABLE_BASE_ID", "appDEgCV6C4vLGjEY")
DEFAULT_TABLE = S.get("AIRTABLE_TABLE", "Sheet1")
DEFAULT_VIEW  = S.get("AIRTABLE_VIEW", "")
DEFAULT_DOMAIN_FIELD = S.get("AIRTABLE_DOMAIN_FIELD", "Domain")

BLOCKLIST_IDS     = S.get("BLOCKLIST_BASE_IDS", "").split(",") if S.get("BLOCKLIST_BASE_IDS") else []
BLOCKLIST_TABLE   = S.get("BLOCKLIST_TABLE_NAME", "tbliCOQZY9RICLsLP")
BLOCKLIST_VIEW_ID = S.get("BLOCKLIST_VIEW_ID", "")
BLOCKLIST_FIELD   = S.get("BLOCKLIST_DOMAIN_FIELD", "Domain")

DEFAULT_GAMBLING = S.get("GAMBLING_DOMAIN", "www.gambling.com")  # Changed default to www.gambling.com

with st.sidebar:
    st.header("Configuration")
    airtable_base_id = st.text_input("Airtable Base ID", value=DEFAULT_BASE)
    airtable_table   = st.text_input("Airtable Table (name or ID)", value=DEFAULT_TABLE)
    airtable_view    = st.text_input("Airtable View (optional name or ID)", value=DEFAULT_VIEW)
    airtable_domain_field = st.text_input("Airtable Domain Field", value=DEFAULT_DOMAIN_FIELD)
    days = st.number_input("Window (days)", min_value=1, max_value=60, value=14)
    gambling_domain = st.text_input("Gambling.com domain", value=DEFAULT_GAMBLING)
    max_concurrency = st.slider("Ahrefs concurrency", 2, 20, 8)
    show_debug = st.checkbox("Show debug counts", value=False)

    run_btn = st.button("Run comparison", type="primary")
    refresh_cache_btn = st.button("Refresh Gambling.com cache")
    clear_cache_btn = st.button("Clear Gambling.com cache")
    test_api_btn = st.button("Test Ahrefs API", type="secondary")

AHREFS_TOKEN   = os.getenv("AHREFS_API_TOKEN", S.get("AHREFS_API_TOKEN", ""))
AIRTABLE_TOKEN = os.getenv("AIRTABLE_API_KEY",   S.get("AIRTABLE_API_KEY",   ""))

if clear_cache_btn:
    clear_cache_row(gambling_domain); st.success("Cleared Gambling.com cache entry.")

if not AHREFS_TOKEN: st.info("Set AHREFS_API_TOKEN in Secrets or env.")
if not AIRTABLE_TOKEN: st.info("Set AIRTABLE_API_KEY in Secrets or env.")

shared_session = make_session()

# ---------------- API Test ----------------
if test_api_btn:
    if not AHREFS_TOKEN:
        st.error("AHREFS_API_TOKEN is required for testing")
    else:
        st.write("## Testing Ahrefs API...")
        ah = AhrefsClient(AHREFS_TOKEN, session=shared_session)
        
        # Test with the configured domain
        test_result = ah.test_exact_api_call(gambling_domain)
        if test_result["success"]:
            st.success(f"✅ API test successful! Found {test_result['rows_count']} backlinks for {gambling_domain}")
            if show_debug:
                st.json(test_result["data"])
        else:
            st.error(f"❌ API test failed: {test_result['error']}")

# ---------------- pipeline ----------------
def run_pipeline(force_refresh_cache: bool = False):
    st.write("🚀 Starting pipeline...")
    st.write(f"AHREFS_TOKEN present: {bool(AHREFS_TOKEN)}")
    st.write(f"AIRTABLE_TOKEN present: {bool(AIRTABLE_TOKEN)}")
    st.write(f"Configuration: base_id={airtable_base_id}, table={airtable_table}")
    st.write(f"Target domain: {gambling_domain}")
    
    # 1) Competitors
    st.write("## 1) Fetch competitors from primary Airtable…")
    at = AirtableClient(AIRTABLE_TOKEN, airtable_base_id, session=shared_session)
    competitors_raw = at.fetch_domains(airtable_table, airtable_domain_field, view_or_id=(airtable_view or None))
    competitors = sorted({sanitize_target_for_ahrefs(x) for x in competitors_raw if sanitize_target_for_ahrefs(x)})
    st.write(f"Found **{len(competitors)}** competitor domains.")

    # 2) Blocklists
    st.write("## 2) Loading blocklist Airtable bases…")
    blocklist_domains: Set[str] = set()
    load_errors = []
    def load_blocklist(base_id: str) -> Set[str]:
        if not base_id.strip(): return set()
        cli = AirtableClient(AIRTABLE_TOKEN, base_id.strip(), session=shared_session)
        try:
            return set(cli.fetch_domains(BLOCKLIST_TABLE, BLOCKLIST_FIELD, view_or_id=(BLOCKLIST_VIEW_ID or None)))
        except requests.HTTPError as e:
            load_errors.append(str(e)); return set()
    if BLOCKLIST_IDS:
        with ThreadPoolExecutor(max_workers=min(8, len(BLOCKLIST_IDS))) as pool:
            for fut in as_completed([pool.submit(load_blocklist, b) for b in BLOCKLIST_IDS]):
                blocklist_domains.update(fut.result() or set())
    st.write(f"Loaded **{len(blocklist_domains)}** blocklisted domains from **{len([b for b in BLOCKLIST_IDS if b.strip()])}** base(s).")
    if load_errors:
        with st.expander("Blocklist load warnings"):
            for err in load_errors: st.write(f"- {err}")

    # 3) Gambling.com baseline (cache → exact API format → fallback variants)
    st.write("## 3) Gambling.com referring domains (baseline)")
    ah = AhrefsClient(AHREFS_TOKEN, session=shared_session)

    cache_hit = None if force_refresh_cache else load_ref_domains_from_cache(gambling_domain)
    gdc_ref_domains: List[str] = []
    baseline_notes = []

    if cache_hit:
        fetched_at, gdc_ref_domains = cache_hit
        baseline_notes.append(f"Cache hit: {len(gdc_ref_domains)} (cached {fetched_at.isoformat()})")
    else:
        # Use the exact format from Ahrefs interface
        t_naked = sanitize_target_for_ahrefs(gambling_domain) or "www.gambling.com"
        t_www   = "www.gambling.com" if "www." not in t_naked else t_naked
        combos = [(t_naked,"domain"), (t_naked,"subdomains"), (t_www,"domain"), (t_www,"subdomains")]

        parts, errs = [], []
        chosen_variant = None
        with ThreadPoolExecutor(max_workers=4) as pool:
            futs = [pool.submit(ah.baseline_all_backlinks_variants, t, m) for (t,m) in combos]
            for fut in as_completed(futs):
                try:
                    doms, note = fut.result()
                    if doms and not chosen_variant:
                        chosen_variant = note
                    parts.append(doms)
                except Exception as e:
                    errs.append(str(e))
        merged = merge_unique(*parts)
        if merged:
            gdc_ref_domains = merged
            baseline_notes.append(f"Baseline via {chosen_variant} ({len(merged)} domains).")
        else:
            parts2, errs2 = [], []
            with ThreadPoolExecutor(max_workers=4) as pool:
                futs = [pool.submit(ah.baseline_refdomains_relaxed, t, m) for (t,m) in combos]
                for fut in as_completed(futs):
                    try:
                        doms, note = fut.result()
                        if doms and not chosen_variant:
                            chosen_variant = note
                        parts2.append(doms)
                    except Exception as e:
                        errs2.append(str(e))
            merged2 = merge_unique(*parts2)
            gdc_ref_domains = merged2
            if merged2:
                baseline_notes.append(f"Baseline via {chosen_variant} ({len(merged2)} domains).")
            if errs or errs2:
                with st.expander("Baseline fetch warnings/errors"):
                    for e in errs: st.write(e)
                    for e in errs2: st.write(e)

        if gdc_ref_domains:
            save_ref_domains_to_cache(gambling_domain, gdc_ref_domains)

    if baseline_notes:
        for n in baseline_notes: st.write("• " + n)
    st.write(f"Fetched {'**0**' if not gdc_ref_domains else f'**{len(gdc_ref_domains)}**'} referring domains for **{gambling_domain}**.")
    gdc_ref_set = set(gdc_ref_domains)

    # 4) New backlinks per competitor (threaded)
    st.write("## 4) Fetching new backlinks from Ahrefs (last N days)…")
    output_records: List[Dict[str, Any]] = []
    def fetch_comp(comp: str) -> List[Dict[str, Any]]:
        try:
            rows = ah.fetch_new_backlinks_last_n_days(comp, days=days, mode="domain")
            return [{"_debug": f"{comp}: {len(rows)} new"}] + rows
        except requests.HTTPError as e:
            return [{"_error": f"{comp}: {e}"}]
    if competitors:
        prog = st.progress(0.0); done=0
        with ThreadPoolExecutor(max_workers=max_concurrency) as pool:
            for fut in as_completed([pool.submit(fetch_comp, c) for c in competitors]):
                res = fut.result() or []
                for r in res:
                    if "_debug" in r and show_debug: st.write(r["_debug"])
                    elif "_error" in r: st.error(r["_error"])
                    else:
                        src = r.get("source_url")
                        reg = extract_registrable_domain(url_to_host(src)) if src else ""
                        if reg and reg not in gdc_ref_set and reg not in blocklist_domains:
                            output_records.append({
                                "linking_domain": reg,
                                "source_url": src,
                                "first_seen": r.get("first_seen"),
                            })
                done += 1; prog.progress(done/len(competitors))

    # 5) Results
    st.write("## 5) Final results — exclusive domains")
    if not output_records:
        st.success("No exclusive domains found after filtering Gambling.com baseline and blocklists.")
        return
    df = pd.DataFrame.from_records(output_records).drop_duplicates(subset=["linking_domain"])
    st.dataframe(df, use_container_width=True)
    st.download_button("Download CSV", df.to_csv(index=False).encode("utf-8"), "exclusive_domains.csv")

# ---------------- actions ----------------
if run_btn:
    if not (airtable_base_id and airtable_table and AHREFS_TOKEN and AIRTABLE_TOKEN):
        st.error("Please set AIRTABLE_* and AHREFS_API_TOKEN in Secrets or environment.")
    else:
        st.write("🔍 Debug: Button clicked, checking configuration...")
        st.write(f"AHREFS_TOKEN: {'✅ Set' if AHREFS_TOKEN else '❌ Missing'}")
        st.write(f"AIRTABLE_TOKEN: {'✅ Set' if AIRTABLE_TOKEN else '❌ Missing'}")
        st.write(f"Base ID: {airtable_base_id}")
        st.write(f"Table: {airtable_table}")
        st.write(f"Target domain: {gambling_domain}")
        st.write("✅ All configuration looks good, starting pipeline...")
        run_pipeline(force_refresh_cache=False)

if refresh_cache_btn:
    if not AHREFS_TOKEN: st.error("AHREFS_API_TOKEN missing.")
    else: run_pipeline(force_refresh_cache=True)

st.caption("v1.5.6 - Fixed success message to show actual count (~31,576) instead of API limit (50,000). Now displays accurate domain counts for proper baseline comparison.")
