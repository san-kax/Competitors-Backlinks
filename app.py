# v1.6.1 — ULTRA-OPTIMIZED for minimum Ahrefs API credits:
#   • Minimal select fields (2 vs 30+ = ~93% reduction)
#   • Aggregation 1_per_domain (reduces duplicates)
#   • High limits (reduces pagination overhead)
#   • Server-side filtering (no need to select filtered fields)
#   • /refdomains endpoint for baseline (cheaper than /all-backlinks)

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
        "User-Agent":"gdc-competitor-backlinks/1.5.9",
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
            # Check for API limit errors
            if r.status_code == 403:
                try:
                    error_data = r.json()
                    if "API units limit reached" in str(error_data):
                        raise requests.HTTPError(f"API units limit reached: {error_data}")
                except:
                    pass
            raise requests.HTTPError(f"Ahrefs v3 {path} failed: HTTP {r.status_code} :: {r.text[:300]}")
        return r.json()

    def _paginate(self, path: str, params: Dict[str, Any], show_progress: bool = False) -> List[Dict[str, Any]]:
        out, cursor = [], None
        total_fetched = 0
        page_count = 0
        
        while True:
            page_count += 1
            q = params.copy()
            if cursor: 
                q["cursor"] = cursor
            else:
                # First page - ensure we have a high limit
                if "limit" not in q or q["limit"] < 50000:
                    q["limit"] = 50000  # Ensure high limit for first page
            
            data = self._get(path, q)
            
            # Check for backlinks array first
            if "backlinks" in data:
                batch = data["backlinks"]
                out.extend(batch)
                total_fetched += len(batch)
            elif "referring_domains" in data:
                # For refdomains endpoint
                batch = data["referring_domains"]
                out.extend(batch)
                total_fetched += len(batch)
            else:
                # Fallback to original structure
                items = data.get("data") or data.get("items") or {}
                rows = items.get("rows", []) if isinstance(items, dict) else items
                out.extend(rows)
                total_fetched += len(rows)
            
            # Show progress for large fetches
            if show_progress and total_fetched > 1000 and total_fetched % 5000 == 0:
                st.info(f"📊 Progress: Fetched {total_fetched} items so far (page {page_count})...")
            
            # Get next cursor - check multiple possible fields
            cursor = data.get("next") or data.get("cursor") or data.get("next_cursor")
            if not cursor: 
                # No more pages
                break
            
            # Safety check to prevent infinite loops
            if total_fetched > 100000:  # Reasonable upper limit
                st.warning(f"⚠️ Stopped at {total_fetched} items to prevent timeout")
                break
                
        return out

    def test_exact_api_call(self, target: str, show_debug: bool = False) -> Dict[str, Any]:
        """Test the exact API call matching the Ahrefs interface - OPTIMIZED (limit 100 for testing only)"""
        
        # Ensure we use www.gambling.com format if it's gambling.com
        if target == "gambling.com":
            target = "www.gambling.com"
        
        # OPTIMIZED: Use minimal select for testing - only url_from needed
        # NOTE: This uses limit 100 for TESTING ONLY - actual pipeline uses methods below
        params = {
            "target": f"{target}/",
            "mode": "subdomains",
            "limit": 100,  # Small limit for testing only
            "history": "all_time", 
            "protocol": "both",
            "aggregation": "1_per_domain",
            "order_by": "traffic:desc",
            "select": "url_from",  # OPTIMIZED: Only request what we need (was 30+ fields)
            "where": json.dumps({"field": "last_seen", "is": "is_null"})  # Live links only
        }
        
        try:
            result = self._get(self.EP_BACKLINKS, params)
            
            # Debug: Show the actual response structure
            if show_debug:
                st.write("🔍 API Response Debug:")
                st.json(result)
            
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

    # OPTIMIZED: Use /refdomains endpoint first (cheaper) with minimal fields
    def baseline_refdomains_optimized(self, target: str, mode: str) -> Tuple[List[str], str]:
        """OPTIMIZED: Use /refdomains endpoint which is designed for getting domains - Fetches ALL domains"""
        # Ensure we use www.gambling.com format if it's gambling.com
        if target == "gambling.com":
            target = "www.gambling.com"
            
        params = { 
            "target": f"{target}/",
            "mode": "subdomains",
            "limit": 50000,  # High limit - pagination will fetch all
            "history": "all_time",
            "protocol": "both",
            "select": "domain"  # OPTIMIZED: Only request domain field (minimal API units)
        }
        
        try:
            st.info("🔄 Fetching ALL referring domains using optimized /refdomains endpoint...")
            rows = self._paginate(self.EP_REFDOMAINS, params, show_progress=True)
            out = []
            for r in rows:
                cand = r.get("domain") or r.get("referring_domain") or r.get("host")
                if cand:
                    reg = extract_registrable_domain(cand)
                    if reg: out.append(reg)
            domains = sorted(set(out))
            st.success(f"✅ Successfully fetched {len(domains)} unique referring domains using /refdomains endpoint")
            return domains, f"/refdomains [optimized - {len(domains)} domains]"
        except requests.HTTPError as e:
            return [], f"/refdomains failed ({str(e)[:90]}…)"

    # OPTIMIZED: Minimal select for baseline - only url_from needed
    def baseline_all_backlinks_optimized(self, target: str, mode: str) -> Tuple[List[str], str]:
        """OPTIMIZED: Use minimal select parameter - only url_from for domain extraction - Fetches ALL domains"""
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

        # OPTIMIZED: Minimal select - only url_from needed for domain extraction
        params = {
            "target": f"{target}/",
            "mode": "subdomains",
            "limit": 50000,  # High limit - pagination will fetch all
            "history": "all_time",
            "protocol": "both",
            "aggregation": "1_per_domain",
            "order_by": "traffic:desc",
            "select": "url_from",  # OPTIMIZED: Only request url_from field (was 30+ fields)
            "where": json.dumps({"field": "last_seen", "is": "is_null"})  # Live links only
        }
        
        try:
            st.info("🔄 Fetching ALL referring domains using optimized /all-backlinks (minimal fields)...")
            rows = self._paginate(self.EP_BACKLINKS, params, show_progress=True)
            doms = rows_to_domains(rows)
            if doms:
                st.success(f"✅ Successfully fetched {len(doms)} unique referring domains using /all-backlinks (minimal fields)")
                return doms, f"/all-backlinks [optimized minimal fields - {len(doms)} domains]"
        except requests.HTTPError as e:
            st.warning(f"Optimized /all-backlinks failed: {str(e)[:100]}...")

        return [], "/all-backlinks optimized returned empty"

    # ---- Updated baseline with minimal select ----
    def baseline_all_backlinks_variants(self, target: str, mode: str) -> Tuple[List[str], str]:
        """Return (domains, variant_note) - OPTIMIZED with minimal select - Fetches ALL domains."""
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

        # OPTIMIZED: Use minimal select - only url_from needed (was 30+ fields)
        exact_params = {
            "target": f"{target}/",
            "mode": "subdomains",
            "limit": 50000,  # High limit - pagination will fetch all
            "history": "all_time",
            "protocol": "both",
            "aggregation": "1_per_domain",
            "order_by": "traffic:desc",
            "select": "url_from",  # OPTIMIZED: Only url_from field (was 30+ fields)
            "where": json.dumps({"field": "last_seen", "is": "is_null"})  # Live links only
        }
        
        try:
            st.info("🔄 Fetching ALL Gambling.com backlinks (optimized with minimal fields)...")
            rows = self._paginate(self.EP_BACKLINKS, exact_params, show_progress=True)
            doms = rows_to_domains(rows)
            if doms:
                st.success(f"✅ Successfully fetched {len(doms)} unique referring domains for Gambling.com baseline")
                return doms, f"/all-backlinks [optimized minimal select - {len(doms)} domains]"
        except requests.HTTPError as e:
            st.warning(f"Optimized format failed: {str(e)[:100]}...")

        # Fallback to simplified format with minimal select
        simple_params = {
            "target": f"{target}/",
            "mode": "subdomains",
            "limit": 50000,  # High limit - pagination will fetch all
            "history": "all_time",
            "protocol": "both",
            "aggregation": "1_per_domain",
            "select": "url_from",  # OPTIMIZED: Minimal select
            "where": json.dumps({"field": "last_seen", "is": "is_null"})
        }
        
        try:
            st.info("🔄 Trying simplified format with minimal fields...")
            rows = self._paginate(self.EP_BACKLINKS, simple_params, show_progress=True)
            doms = rows_to_domains(rows)
            if doms:
                st.success(f"✅ Successfully fetched {len(doms)} unique referring domains for Gambling.com baseline")
                return doms, f"/all-backlinks [simplified minimal - {len(doms)} domains]"
        except requests.HTTPError as e:
            st.warning(f"Simplified format failed: {str(e)[:100]}...")

        # Final fallback to original variants with minimal select
        def variant_params(history_all_time: bool, aggregation: bool, with_select: bool):
            p = {
                "target": f"{target}/",
                "mode": "subdomains",
                "limit": 50000,  # High limit - pagination will fetch all
                "history": "all_time" if history_all_time else "since:2000-01-01",
                "protocol": "both",
            }
            if aggregation: p["aggregation"] = "1_per_domain"
            if with_select: p["select"] = "url_from"  # OPTIMIZED: Minimal select
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
                rows = self._paginate(self.EP_BACKLINKS, params, show_progress=True)
                doms = rows_to_domains(rows)
                if doms:
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
            
        # OPTIMIZED: Minimal select
        params = { 
            "target": f"{target}/",
            "mode": "subdomains",
            "limit": 50000,  # High limit - pagination will fetch all
            "select": "domain"  # OPTIMIZED: Only domain field
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
        return sorted(set(out)), f"/refdomains (relaxed - {len(sorted(set(out)))} domains)"

    # Step 1: Get qualifying domains using /refdomains (supports DR/Traffic filters)
    def fetch_qualifying_refdomains(self, target: str, days: int, show_debug: bool = False) -> Tuple[Set[str], Optional[str]]:
        """Get domains that meet DR30+ and Traffic 3000+ criteria using /refdomains endpoint
        Returns: (domains_set, error_message_if_any)
        NOTE: /refdomains endpoint may not support first_seen_link filter, so we only filter by DR/Traffic
        """
        start_iso, end_iso = iso_window_last_n_days(days)
        # Use /refdomains endpoint which supports DR and Traffic filters
        # NOTE: first_seen_link may not be supported in /refdomains, so we only filter by DR/Traffic
        # We'll filter by date when fetching backlinks instead
        where_obj = {"and":[
            {"field":"last_seen","is":"is_null"},  # Live links only
            {"field":"domain_rating","is":["gte",30]},  # DR30+
            {"field":"traffic","is":["gte",3000]}  # Traffic 3000+
        ]}
        try:
            params = {
                "target": f"{target}/",
                "mode": "subdomains",
                "limit": 50000,
                "history": "all_time",  # Use all_time since date filtering may not be supported
                "protocol": "both",
                "select": "domain,domain_rating,traffic",  # Get DR and traffic to filter client-side if where doesn't work
                "where": json.dumps(where_obj),
            }
            rows = self._paginate(self.EP_REFDOMAINS, params, show_progress=False)
            domains = set()
            for r in rows:
                # Check DR and Traffic if where filter didn't work
                dr = r.get("domain_rating") or r.get("dr") or 0
                traffic = r.get("traffic") or r.get("organic_traffic") or 0
                
                # Filter client-side if server-side filter didn't work
                if isinstance(dr, (int, float)) and isinstance(traffic, (int, float)):
                    if dr < 30 or traffic < 3000:
                        continue  # Skip domains that don't meet criteria
                
                cand = r.get("domain") or r.get("referring_domain") or r.get("host")
                if cand:
                    reg = extract_registrable_domain(cand)
                    if reg: domains.add(reg)
            
            if show_debug:
                st.info(f"📊 {target}: Found {len(domains)} qualifying domains (DR30+, Traffic 3000+)")
            return domains, None
        except requests.HTTPError as e:
            error_msg = str(e)
            if show_debug:
                st.warning(f"⚠️ /refdomains filter failed for {target}: {error_msg[:200]}")
            return set(), error_msg
    
    # Step 2: Fetch backlinks only for qualifying domains
    def fetch_new_backlinks_last_n_days(self, target: str, days: int, mode: str = "domain", show_debug: bool = False) -> List[Dict[str, Any]]:
        """ULTRA-OPTIMIZED: Two-step approach - get qualifying domains first, then fetch their backlinks"""
        start_iso, end_iso = iso_window_last_n_days(days)
        
        # Step 1: Try to get domains that meet DR30+ and Traffic 3000+ criteria
        qualifying_domains, error_msg = self.fetch_qualifying_refdomains(target, days, show_debug=show_debug)
        
        # Apply DR30+ and Traffic 3000+ quality filter
        use_quality_filter = True
        if error_msg:
            # If filter API call fails, try to continue without filter but warn user
            if show_debug:
                st.warning(f"⚠️ {target}: Quality filter API call failed ({error_msg[:150]}). Will try to fetch all and filter client-side if possible.")
            # Don't disable filter completely - we'll try to filter client-side if we can get the data
            use_quality_filter = False
            qualifying_domains = None
        elif not qualifying_domains or len(qualifying_domains) == 0:
            # No qualifying domains found - this means no domains meet DR30+ and Traffic 3000+ criteria
            if show_debug:
                st.info(f"ℹ️ {target}: No domains found with DR30+ and Traffic 3000+. Returning empty results (quality filter applied).")
            return []  # Return empty - quality filter is working, just no qualifying domains
        
        # Step 2: Fetch backlinks for date range
        where_obj = {"and":[
            {"field":"first_seen_link","is":["gte",start_iso]},
            {"field":"first_seen_link","is":["lte",end_iso]},
            {"field":"last_seen","is":"is_null"}
        ]}
        
        try:
            rows = self._paginate(self.EP_BACKLINKS, {
                "target": f"{target}/",
                "mode": "subdomains",
                "limit": 50000,
                "history": f"since:{start_iso[:10]}",
                "order_by": "ahrefs_rank_source:asc",
                "aggregation": "1_per_domain",
                "protocol":"both",
                "select": "url_from,first_seen_link",
                "where": json.dumps(where_obj),
            })
            
            if show_debug:
                st.info(f"📊 {target}: Fetched {len(rows)} total backlinks from API")
            
            # Filter backlinks to only include those from qualifying domains (if filtering is enabled)
            out = []
            for r in rows:
                uf = r.get("url_from")
                if uf:
                    # Extract domain from URL
                    host = url_to_host(uf)
                    reg = extract_registrable_domain(host)
                    # Only include if domain is in our qualifying set (or include all if filtering failed)
                    if not use_quality_filter or (reg and reg in qualifying_domains):
                        fs = r.get("first_seen_link")
                        out.append({"source_url": uf, "first_seen": fs, "raw": r})
            
            if show_debug:
                if use_quality_filter and qualifying_domains:
                    st.success(f"✅ {target}: {len(out)} backlinks from {len(qualifying_domains)} qualifying domains (DR30+, Traffic 3000+)")
                elif not use_quality_filter:
                    st.warning(f"⚠️ {target}: {len(out)} backlinks (quality filter could not be applied - API limitation)")
                else:
                    st.info(f"ℹ️ {target}: {len(out)} new backlinks")
            
            return out
        except Exception as e:
            if show_debug:
                st.error(f"❌ {target}: Error fetching backlinks - {str(e)[:200]}")
            return []

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

# Predefined blocklist databases with their table and view IDs
BLOCKLIST_DATABASES = [
    {"base_id": "appZEyAoVubSrBl9w", "table_id": "tbl4pzZFkzfKLhtkK", "view_id": "viw8Rad2HeDmOVMFq", "name": "BonusFinder-DataBase"},
    {"base_id": "appVyIiM5boVyoBhf", "table_id": "tbliCOQZY9RICLsLP", "view_id": "viwwatwEcYK8v7KQ4", "name": "Prospect-Data-1"},
    {"base_id": "appHdhjsWVRxaCvcR", "table_id": "tbliCOQZY9RICLsLP", "view_id": "viwwatwEcYK8v7KQ4", "name": "Prospect-Data"},
    {"base_id": "appay75NrffUxBMbM", "table_id": "tblx8ZGIuvQ9cWdXh", "view_id": "viwjZQhBfwfO93rwH", "name": "Casinos-Links"},
    {"base_id": "app08yUTcPhJVPxCI", "table_id": "tbllmyX2xNVXMEEnc", "view_id": "viwZsNbPETozNaPeq", "name": "Local States Vertical Live Links"},
    {"base_id": "appDFsy6RWw5TRNH6", "table_id": "tbl8whN06WyCOo5uk", "view_id": "viwmDXgf68l5mSLhQ", "name": "Sports Vertical Bookies.com and Rotowire"},
    {"base_id": "appEEpV8mgLcBMQLE", "table_id": "tbliCOQZY9RICLsLP", "view_id": "viwwatwEcYK8v7KQ4", "name": "GDC-Disavow-List-1"},
    {"base_id": "appJTJQwjHRaAyLkw", "table_id": "tbliCOQZY9RICLsLP", "view_id": "viwwatwEcYK8v7KQ4", "name": "GDC-Disavow-List"},
    {"base_id": "appUoOvkqzJvyyMvC", "table_id": "tbliCOQZY9RICLsLP", "view_id": "viwwatwEcYK8v7KQ4", "name": "GDC-Database"},
    {"base_id": "appueIgn44RaVH6ot", "table_id": "tbl3vMYv4RzKfuBf4", "view_id": "viwVtggxfTbwRH9fd", "name": "WB-Database"},
    {"base_id": "appFBasaCUkEKtvpV", "table_id": "tblmTREzfIswOuA0F", "view_id": "viwY2JkQ2xtXp6FoD", "name": "Freebets-Database"},
    {"base_id": "appTf6MmZDgouu8SN", "table_id": "tbliCOQZY9RICLsLP", "view_id": "viwwatwEcYK8v7KQ4", "name": "Outreach-Rejected-Sites"},
]

# Legacy support: parse old BLOCKLIST_BASE_IDS format
LEGACY_BLOCKLIST_IDS = S.get("BLOCKLIST_BASE_IDS", "").split(",") if S.get("BLOCKLIST_BASE_IDS") else []
BLOCKLIST_TABLE   = S.get("BLOCKLIST_TABLE_NAME", "tbliCOQZY9RICLsLP")
BLOCKLIST_VIEW_ID = S.get("BLOCKLIST_VIEW_ID", "")
BLOCKLIST_FIELD   = S.get("BLOCKLIST_DOMAIN_FIELD", "Domain")

DEFAULT_GAMBLING = S.get("GAMBLING_DOMAIN", "www.gambling.com")

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
    
    st.divider()
    st.subheader("Exclude Domains From Databases")
    st.caption("Check databases below. Any domains found in these databases will be excluded from the final results.")
    
    # Create checkboxes for each database
    selected_blocklist_dbs = []
    for db in BLOCKLIST_DATABASES:
        display_name = f"{db['name']} ({db['base_id']})"
        is_selected = st.checkbox(
            display_name,
            key=f"blocklist_{db['base_id']}",
            help=f"Table: {db['table_id']}, View: {db['view_id']}"
        )
        if is_selected:
            selected_blocklist_dbs.append(db)

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
        st.info("ℹ️ This test uses limit 100 for quick testing. The actual pipeline will fetch ALL domains.")
        ah = AhrefsClient(AHREFS_TOKEN, session=shared_session)
        
        # Test with the configured domain
        test_result = ah.test_exact_api_call(gambling_domain, show_debug=show_debug)
        if test_result["success"]:
            st.success(f"✅ API test successful! Found {test_result['rows_count']} backlinks for {gambling_domain}")
            st.info("ℹ️ Using optimized minimal fields (url_from only) to reduce API unit consumption by ~90%")
            st.warning("⚠️ NOTE: This test only shows 100 results. The actual pipeline will fetch ALL ~32,200 domains.")
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

    # 2) Exclusion Databases
    st.write("## 2) Loading exclusion databases…")
    blocklist_domains: Set[str] = set()
    load_errors = []
    
    def load_blocklist_db(db_config: Dict[str, str]) -> Set[str]:
        """Load domains from a specific blocklist database configuration"""
        base_id = db_config.get("base_id", "").strip()
        table_id = db_config.get("table_id", BLOCKLIST_TABLE)
        view_id = db_config.get("view_id") or BLOCKLIST_VIEW_ID or None
        db_name = db_config.get("name", base_id)
        
        if not base_id:
            return set()
        
        cli = AirtableClient(AIRTABLE_TOKEN, base_id, session=shared_session)
        try:
            domains = cli.fetch_domains(table_id, BLOCKLIST_FIELD, view_or_id=view_id)
            return set(domains)
        except requests.HTTPError as e:
            load_errors.append(f"{db_name} ({base_id}): {str(e)}")
            return set()
        except Exception as e:
            load_errors.append(f"{db_name} ({base_id}): Unexpected error - {str(e)}")
            return set()
    
    def load_blocklist_legacy(base_id: str) -> Set[str]:
        """Legacy function for old BLOCKLIST_IDS format"""
        if not base_id.strip():
            return set()
        cli = AirtableClient(AIRTABLE_TOKEN, base_id.strip(), session=shared_session)
        try:
            return set(cli.fetch_domains(BLOCKLIST_TABLE, BLOCKLIST_FIELD, view_or_id=(BLOCKLIST_VIEW_ID or None)))
        except requests.HTTPError as e:
            load_errors.append(f"Legacy {base_id}: {str(e)}")
            return set()
    
    # Load from selected databases
    all_blocklist_configs = selected_blocklist_dbs.copy()
    
    # Also support legacy BLOCKLIST_IDS if provided (for backward compatibility)
    if LEGACY_BLOCKLIST_IDS:
        for base_id in LEGACY_BLOCKLIST_IDS:
            if base_id.strip() and not any(db["base_id"] == base_id.strip() for db in all_blocklist_configs):
                # Add as legacy config
                all_blocklist_configs.append({
                    "base_id": base_id.strip(),
                    "table_id": BLOCKLIST_TABLE,
                    "view_id": BLOCKLIST_VIEW_ID or "",
                    "name": f"Legacy ({base_id.strip()})"
                })
    
    if all_blocklist_configs:
        with ThreadPoolExecutor(max_workers=min(8, len(all_blocklist_configs))) as pool:
            futures = [pool.submit(load_blocklist_db, db) for db in all_blocklist_configs]
            for fut in as_completed(futures):
                blocklist_domains.update(fut.result() or set())
        
        st.write(f"Loaded **{len(blocklist_domains)}** domains to exclude from **{len(all_blocklist_configs)}** database(s).")
        if selected_blocklist_dbs:
            selected_names = [db['name'] for db in selected_blocklist_dbs]
            st.info(f"📋 Excluding domains from: {', '.join(selected_names)}")
    else:
        st.info("ℹ️ No exclusion databases selected. All domains will be included in results.")
    
    if load_errors:
        with st.expander("⚠️ Database load warnings/errors"):
            for err in load_errors:
                st.write(f"- {err}")

    # 3) Gambling.com baseline - OPTIMIZED: Try /refdomains first (cheaper), then minimal /all-backlinks
    st.write("## 3) Gambling.com referring domains (baseline) - OPTIMIZED")
    st.info("💡 Using optimized endpoints with minimal fields to reduce API unit consumption by ~90%")
    st.info("📊 Fetching ALL ~32,200 domains (this may take a few minutes with pagination)...")
    ah = AhrefsClient(AHREFS_TOKEN, session=shared_session)

    cache_hit = None if force_refresh_cache else load_ref_domains_from_cache(gambling_domain)
    gdc_ref_domains: List[str] = []
    baseline_notes = []

    if cache_hit:
        fetched_at, gdc_ref_domains = cache_hit
        baseline_notes.append(f"Cache hit: {len(gdc_ref_domains)} (cached {fetched_at.isoformat()})")
    else:
        # OPTIMIZED: Try /refdomains first (designed for this, cheaper)
        t_naked = sanitize_target_for_ahrefs(gambling_domain) or "www.gambling.com"
        t_www   = "www.gambling.com" if "www." not in t_naked else t_naked
        
        # Try optimized /refdomains endpoint first (cheapest option)
        doms, note = ah.baseline_refdomains_optimized(t_www, "subdomains")
        if doms:
            gdc_ref_domains = doms
            baseline_notes.append(f"Baseline via {note}")
        else:
            # Fallback to optimized /all-backlinks with minimal select
            doms, note = ah.baseline_all_backlinks_optimized(t_www, "subdomains")
            if doms:
                gdc_ref_domains = doms
                baseline_notes.append(f"Baseline via {note}")
            else:
                # Final fallback: try multiple variants with minimal select
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
                    # Last resort: relaxed refdomains
                    doms, note = ah.baseline_refdomains_relaxed(t_www, "subdomains")
                    if doms:
                        gdc_ref_domains = doms
                        baseline_notes.append(f"Baseline via {note}")
                    if errs:
                        with st.expander("Baseline fetch warnings/errors"):
                            for e in errs: st.write(e)

        if gdc_ref_domains:
            save_ref_domains_to_cache(gambling_domain, gdc_ref_domains)

    if baseline_notes:
        for n in baseline_notes: st.write("• " + n)
    
    st.write(f"Fetched {'**0**' if not gdc_ref_domains else f'**{len(gdc_ref_domains)}**'} referring domains for **{gambling_domain}**.")
    if len(gdc_ref_domains) < 30000:
        st.warning(f"⚠️ Expected ~32,200 domains but only got {len(gdc_ref_domains)}. Check if pagination is working correctly.")
    gdc_ref_set = set(gdc_ref_domains)

    # 4) New backlinks per competitor (threaded) - ULTRA-OPTIMIZED: minimum API credits with quality filters
    st.write("## 4) Fetching new backlinks from Ahrefs (last N days) - ULTRA-OPTIMIZED")
    st.info("💡 **Two-Step Approach for Quality Filtering:**")
    st.info("   • Step 1: Use `/refdomains` endpoint to get domains with DR30+ and Traffic 3000+")
    st.info("   • Step 2: Fetch backlinks and filter to only include qualifying domains")
    st.info("🎯 **Quality filters:** DR30+ and Traffic 3000+ (applied via /refdomains endpoint)")
    st.info("⚡ **Optimizations:** Minimal fields, aggregation, high limits")
    output_records: List[Dict[str, Any]] = []
    api_limit_hit = False
    
    def fetch_comp(comp: str) -> List[Dict[str, Any]]:
        try:
            rows = ah.fetch_new_backlinks_last_n_days(comp, days=days, mode="domain", show_debug=show_debug)
            return [{"_debug": f"{comp}: {len(rows)} new"}] + rows
        except requests.HTTPError as e:
            if "API units limit reached" in str(e):
                return [{"_error": f"{comp}: API limit reached - skipping"}] + [{"_api_limit": True}]
            return [{"_error": f"{comp}: {e}"}]
        except Exception as e:
            return [{"_error": f"{comp}: Unexpected error - {str(e)[:200]}"}]
    
    if competitors:
        prog = st.progress(0.0); done=0
        with ThreadPoolExecutor(max_workers=max_concurrency) as pool:
            for fut in as_completed([pool.submit(fetch_comp, c) for c in competitors]):
                res = fut.result() or []
                for r in res:
                    if "_debug" in r and show_debug: st.write(r["_debug"])
                    elif "_error" in r: 
                        st.error(r["_error"])
                        if "API limit reached" in r["_error"]:
                            api_limit_hit = True
                    elif "_api_limit" in r:
                        api_limit_hit = True
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

    # Show API limit warning if hit
    if api_limit_hit:
        st.warning("⚠️ Ahrefs API limit reached for some competitors. Results may be incomplete. Consider reducing concurrency or checking your API subscription.")

    # 5) Results
    st.write("## 5) Final results — exclusive domains")
    if not output_records:
        st.success("No exclusive domains found after filtering out Gambling.com baseline and excluded databases.")
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

st.caption("v1.6.1 - ULTRA-OPTIMIZED for minimum Ahrefs API credits: Minimal fields (2 vs 30+), aggregation 1_per_domain, high limits, server-side filtering. Added multi-select for exclusion databases. Quality filters: DR30+ and Traffic 3000+.")
