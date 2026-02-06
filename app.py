# v3.0.6 — AUTH FIX + HONEST ERROR REPORTING:
#   • Uses /batch-analysis endpoint to get DR/Traffic for many domains in ONE call
#   • Fetches new backlinks per competitor, collects unique domains
#   • Single batch call for metrics (vs individual calls per domain)
#   • Parallel Airtable fetches (competitors + blocklists simultaneously)
#   • Optimized connection pooling and timeouts

import os
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Set, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import pandas as pd
import streamlit as st
import tldextract
from urllib.parse import urlparse

# ---------------- Performance: LRU cache for domain extraction ----------------
@lru_cache(maxsize=50000)
def extract_registrable_domain(url_or_host: str) -> str:
    """Extract registrable domain with LRU caching for repeated lookups."""
    if not url_or_host:
        return ""
    ext = tldextract.extract(url_or_host)
    if not ext.suffix:
        return (ext.domain or "").lower()
    return f"{ext.domain}.{ext.suffix}".lower()

@lru_cache(maxsize=50000)
def url_to_host(url: str) -> str:
    """Extract host from URL with LRU caching."""
    try:
        p = urlparse(url)
        return p.netloc or url
    except Exception:
        return url

def iso_window_last_n_days(days: int) -> Tuple[str, str]:
    """Return ISO timestamps for date window."""
    end = datetime.now(timezone.utc).replace(microsecond=0)
    start = (end - timedelta(days=days)).replace(hour=0, minute=0, second=0)
    return (
        start.isoformat().replace("+00:00", "Z"),
        end.isoformat().replace("+00:00", "Z"),
    )

def sanitize_target_for_ahrefs(val: str) -> Optional[str]:
    """Sanitize domain for Ahrefs API."""
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

def make_session(pool_size: int = 100) -> requests.Session:
    """Create optimized HTTP session with connection pooling."""
    sess = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.3,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retries,
        pool_connections=pool_size,
        pool_maxsize=pool_size,
        pool_block=False
    )
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    sess.headers.update({
        "Accept-Encoding": "gzip, deflate, br",
        "User-Agent": "gdc-competitor-backlinks/3.0.0",
        "Connection": "keep-alive",
    })
    return sess

# ---------------- Airtable Client ----------------
class AirtableClient:
    def __init__(self, api_key: str, base_id: str):
        self.base_url = f"https://api.airtable.com/v0/{base_id}"
        # Uses its own session to avoid overwriting auth headers on a shared session
        self.session = make_session()
        self.session.headers.update({"Authorization": f"Bearer {api_key}"})

    def fetch_domains(self, table_or_id: str, domain_field: str,
                      view_or_id: Optional[str] = None, max_records: int = 20000) -> List[str]:
        """Fetch domains from Airtable with pagination."""
        url = f"{self.base_url}/{requests.utils.quote(table_or_id)}"
        params = {"pageSize": 100, "maxRecords": max_records, "fields[]": domain_field}
        if view_or_id:
            params["view"] = view_or_id

        rows, offset = [], None
        while True:
            if offset:
                params["offset"] = offset
            r = self.session.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            for rec in data.get("records", []):
                v = rec.get("fields", {}).get(domain_field)
                if not v:
                    continue
                for x in (v if isinstance(v, list) else [v]):
                    host = url_to_host(str(x))
                    reg = extract_registrable_domain(host)
                    if reg:
                        rows.append(reg)
            offset = data.get("offset")
            if not offset or len(rows) >= max_records:
                break
        return sorted(set(rows))

# ---------------- Ahrefs Client v3.0 - Batch Analysis API ----------------
class AhrefsClient:
    BASE = "https://api.ahrefs.com/v3"
    EP_BACKLINKS = "/site-explorer/all-backlinks"
    EP_REFDOMAINS = "/site-explorer/refdomains"
    EP_BATCH_ANALYSIS = "/batch-analysis/batch-analysis"

    def __init__(self, token: str, session: Optional[requests.Session] = None):
        self.session = session or make_session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        })
        self._domain_metrics_cache: Dict[str, Dict[str, Any]] = {}

    def _get(self, path: str, params: Dict[str, Any], timeout: int = 90) -> Dict[str, Any]:
        """Execute GET request with error handling."""
        r = self.session.get(f"{self.BASE}{path}", params=params, timeout=timeout)
        if not r.ok:
            error_text = r.text[:300] if r.text else "No error details"
            if r.status_code == 401:
                # Unauthorized - API doesn't have access to this domain
                raise requests.HTTPError(f"UNAUTHORIZED: No API access to query this domain (HTTP 401)")
            if r.status_code == 403:
                try:
                    error_data = r.json()
                    if "API units limit reached" in str(error_data):
                        raise requests.HTTPError(f"API units limit reached: {error_data}")
                except json.JSONDecodeError:
                    pass
            raise requests.HTTPError(f"Ahrefs v3 {path} failed: HTTP {r.status_code} :: {error_text}")
        return r.json()

    def _post(self, path: str, json_data: Dict[str, Any], timeout: int = 120) -> Dict[str, Any]:
        """Execute POST request with error handling."""
        r = self.session.post(f"{self.BASE}{path}", json=json_data, timeout=timeout)
        if not r.ok:
            error_text = r.text[:500] if r.text else "No error details"
            if r.status_code == 401:
                raise requests.HTTPError(f"UNAUTHORIZED: No API access (HTTP 401)")
            if r.status_code == 403:
                try:
                    error_data = r.json()
                    if "API units limit reached" in str(error_data):
                        raise requests.HTTPError(f"API units limit reached: {error_data}")
                except json.JSONDecodeError:
                    pass
            raise requests.HTTPError(f"Ahrefs v3 POST {path} failed: HTTP {r.status_code} :: {error_text}")
        return r.json()

    def _paginate(self, path: str, params: Dict[str, Any],
                  max_items: int = 200000, show_progress: bool = False) -> List[Dict[str, Any]]:
        """Paginate through API results."""
        out, cursor = [], None
        total_fetched = 0
        page_count = 0

        while True:
            page_count += 1
            q = params.copy()
            if cursor:
                q["cursor"] = cursor

            data = self._get(path, q)

            if "backlinks" in data:
                batch = data["backlinks"]
            elif "refdomains" in data:
                batch = data["refdomains"]
            elif "referring_domains" in data:
                batch = data["referring_domains"]
            else:
                items = data.get("data") or data.get("items") or {}
                batch = items.get("rows", []) if isinstance(items, dict) else items

            out.extend(batch)
            total_fetched += len(batch)

            if show_progress and total_fetched > 0 and page_count % 3 == 0:
                st.info(f"📊 Progress: {total_fetched:,} items (page {page_count})...")

            cursor = data.get("next") or data.get("cursor") or data.get("next_cursor")
            if not cursor or total_fetched >= max_items:
                break

        return out

    def get_baseline_domains(self, target: str) -> Tuple[List[str], str]:
        """Get baseline referring domains using /refdomains endpoint."""
        if target == "gambling.com":
            target = "www.gambling.com"

        # Clean target — no trailing slash, no protocol
        target_clean = target.rstrip("/")
        if target_clean.startswith("http://"):
            target_clean = target_clean[7:]
        elif target_clean.startswith("https://"):
            target_clean = target_clean[8:]

        # Ahrefs API has no hard cap on limit — fetch all in one call
        params = {
            "target": target_clean,
            "mode": "subdomains",
            "limit": 200000,
            "output": "json",
            "select": "domain",
            "order_by": "domain_rating:desc",
        }

        try:
            st.info(f"🔄 Fetching baseline referring domains for `{target_clean}`...")
            data = self._get(self.EP_REFDOMAINS, params, timeout=300)
            rows = data.get("refdomains", [])

            if not rows:
                st.warning(f"⚠️ /refdomains returned 0 rows for `{target_clean}`")
                return [], f"/refdomains returned 0 rows"

            domains = []
            for r in rows:
                cand = r.get("domain") or r.get("referring_domain") or r.get("host")
                if cand:
                    reg = extract_registrable_domain(cand)
                    if reg:
                        domains.append(reg)
            unique_domains = sorted(set(domains))
            st.success(f"✅ Fetched {len(unique_domains):,} baseline domains")
            return unique_domains, f"/refdomains [{len(unique_domains):,} domains]"
        except requests.HTTPError as e:
            st.error(f"❌ /refdomains failed: {str(e)[:200]}")
            return [], f"/refdomains failed ({str(e)[:90]}…)"

    def batch_get_domain_metrics(
        self,
        domains: List[str],
        batch_size: int = 100,
        show_debug: bool = False
    ) -> Dict[str, Dict[str, Any]]:
        """
        BATCH ANALYSIS API: Get DR and Traffic for multiple domains in batches.

        This is MUCH more efficient than individual calls.
        Max 100 domains per batch per API docs.
        """
        # Enforce API limit
        batch_size = min(batch_size, 100)
        if not domains:
            return {}

        results: Dict[str, Dict[str, Any]] = {}
        total_batches = (len(domains) + batch_size - 1) // batch_size

        for i in range(0, len(domains), batch_size):
            batch = domains[i:i + batch_size]
            batch_num = (i // batch_size) + 1

            if show_debug:
                st.info(f"📊 Batch {batch_num}/{total_batches}: Processing {len(batch)} domains...")

            # Build targets for batch analysis
            # API requires: url (required), mode (required), protocol (required)
            targets = []
            for domain in batch:
                targets.append({
                    "url": f"https://{domain}",
                    "mode": "subdomains",  # exact, prefix, domain, or subdomains
                    "protocol": "both"  # both, http, or https
                })

            payload = {
                "targets": targets,
                "select": ["url", "domain_rating", "org_traffic"],
                "volume_mode": "monthly"
            }

            try:
                response = self._post(self.EP_BATCH_ANALYSIS, payload)

                # Parse response - structure is {"targets": [...]}
                targets_data = response.get("targets", [])
                for item in targets_data:
                    url = item.get("url", "")
                    # Extract domain from URL
                    domain = extract_registrable_domain(url_to_host(url))
                    if domain:
                        dr = item.get("domain_rating", 0)
                        traffic = item.get("org_traffic", 0)

                        # Handle None/null values
                        try:
                            dr = int(dr) if dr is not None else 0
                            traffic = int(traffic) if traffic is not None else 0
                        except (ValueError, TypeError):
                            dr = 0
                            traffic = 0

                        results[domain] = {
                            "dr": dr,
                            "traffic": traffic
                        }
                        # Also cache it
                        self._domain_metrics_cache[domain] = results[domain]

            except requests.HTTPError as e:
                error_msg = str(e)
                is_auth_error = "UNAUTHORIZED" in error_msg or "401" in error_msg
                if show_debug:
                    st.warning(f"⚠️ Batch {batch_num} failed: {error_msg[:100]}")
                if is_auth_error:
                    # Don't silently default to zeros on auth errors — propagate clearly
                    st.error(f"❌ Batch Analysis API returned 401 Unauthorized. "
                             f"Check your API key and plan permissions.")
                    for domain in batch:
                        if domain not in results:
                            results[domain] = {"dr": None, "traffic": None, "error": "unauthorized"}
                else:
                    # Non-auth errors: set defaults for this batch
                    for domain in batch:
                        if domain not in results:
                            results[domain] = {"dr": 0, "traffic": 0}

        successful = sum(1 for v in results.values() if v.get("error") is None)
        if show_debug:
            if successful == len(results):
                st.success(f"✅ Got metrics for {len(results)} domains via Batch Analysis API")
            else:
                st.warning(f"⚠️ Got metrics for {successful}/{len(results)} domains "
                           f"({len(results) - successful} failed with auth errors)")

        return results

    def fetch_new_backlinks_raw(
        self,
        target: str,
        days: int,
        show_debug: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Fetch new backlinks for a competitor using Ahrefs API v3.

        Uses /site-explorer/all-backlinks ordered by first_seen_link desc,
        then filters client-side to only include backlinks within the date window.
        (The v3 API does not have /new-backlinks or /new-refdomains endpoints.)
        """
        start_iso, end_iso = iso_window_last_n_days(days)
        start_date = start_iso[:10]  # YYYY-MM-DD

        # Clean target - remove protocol if present
        target_clean = target
        if target_clean.startswith("http://"):
            target_clean = target_clean[7:]
        elif target_clean.startswith("https://"):
            target_clean = target_clean[8:]
        target_clean = target_clean.rstrip("/")

        if show_debug:
            st.write(f"🔍 {target_clean}: Calling API with params...")

        try:
            # Ahrefs has no hard cap on limit — fetch enough to cover the date window
            params = {
                "target": target_clean,
                "mode": "subdomains",
                "limit": 50000,
                "output": "json",
                "select": "url_from,first_seen_link",
                "order_by": "first_seen_link:desc",
            }

            data = self._get(self.EP_BACKLINKS, params, timeout=120)
            rows = data.get("backlinks", [])

            if show_debug:
                st.info(f"📊 {target_clean}: /all-backlinks returned {len(rows)} backlinks")

        except requests.HTTPError as e:
            last_error = str(e)
            if show_debug:
                st.error(f"❌ {target_clean}: API call failed - {last_error[:150]}")
            return []

        # Filter to only backlinks within the date window
        results = []
        for r in rows:
            url = r.get("url_from") or ""
            if not url:
                continue

            host = url_to_host(url)
            reg = extract_registrable_domain(host)
            if not reg:
                continue

            first_seen = r.get("first_seen_link") or ""

            # Only include backlinks first seen within our date window
            if first_seen and len(first_seen) >= 10:
                first_seen_date = first_seen[:10]
                if first_seen_date >= start_date:
                    results.append({
                        "source_url": url,
                        "first_seen": first_seen,
                        "domain": reg
                    })
                else:
                    # Since results are ordered by first_seen_link desc,
                    # once we see a date before our window, we can stop
                    break

        if show_debug:
            st.success(f"✅ {target_clean}: {len(results)} new backlinks/domains found")

        return results

# ---------------- SQLite Cache ----------------
DB_PATH = "./backlink_cache.sqlite"

def ensure_db():
    """Ensure database and tables exist."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ref_domain_cache (
                target_domain TEXT NOT NULL PRIMARY KEY,
                fetched_at TEXT NOT NULL,
                domains_json TEXT NOT NULL
            )
        """)
        conn.commit()

def save_ref_domains_to_cache(target_domain: str, domains: List[str]):
    """Save domains to cache."""
    if not domains:
        return
    ensure_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "REPLACE INTO ref_domain_cache (target_domain, fetched_at, domains_json) VALUES (?, ?, ?)",
            (target_domain, datetime.utcnow().isoformat(), json.dumps(domains))
        )
        conn.commit()

def load_ref_domains_from_cache(target_domain: str) -> Optional[Tuple[datetime, List[str]]]:
    """Load domains from cache."""
    ensure_db()
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            "SELECT fetched_at, domains_json FROM ref_domain_cache WHERE target_domain = ?",
            (target_domain,)
        )
        row = cur.fetchone()
        if row:
            return (datetime.fromisoformat(row[0]), json.loads(row[1]))
        return None

def clear_cache_row(target_domain: str):
    """Clear specific cache entry."""
    ensure_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM ref_domain_cache WHERE target_domain = ?", (target_domain,))
        conn.commit()

# ---------------- Streamlit UI ----------------
st.set_page_config(page_title="Competitor Backlinks Analyzer v3.0", layout="wide")
S = st.secrets if hasattr(st, "secrets") else {}

DEFAULT_BASE = S.get("AIRTABLE_BASE_ID", "appDEgCV6C4vLGjEY")
DEFAULT_TABLE = S.get("AIRTABLE_TABLE", "Sheet1")
DEFAULT_VIEW = S.get("AIRTABLE_VIEW", "")
DEFAULT_DOMAIN_FIELD = S.get("AIRTABLE_DOMAIN_FIELD", "Domain")

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

LEGACY_BLOCKLIST_IDS = S.get("BLOCKLIST_BASE_IDS", "").split(",") if S.get("BLOCKLIST_BASE_IDS") else []
BLOCKLIST_TABLE = S.get("BLOCKLIST_TABLE_NAME", "tbliCOQZY9RICLsLP")
BLOCKLIST_VIEW_ID = S.get("BLOCKLIST_VIEW_ID", "")
BLOCKLIST_FIELD = S.get("BLOCKLIST_DOMAIN_FIELD", "Domain")
DEFAULT_GAMBLING = S.get("GAMBLING_DOMAIN", "www.gambling.com")

# Sidebar
with st.sidebar:
    st.header("⚙️ Configuration")

    airtable_base_id = st.text_input("Airtable Base ID", value=DEFAULT_BASE)
    airtable_table = st.text_input("Airtable Table", value=DEFAULT_TABLE)
    airtable_view = st.text_input("Airtable View (optional)", value=DEFAULT_VIEW)
    airtable_domain_field = st.text_input("Domain Field", value=DEFAULT_DOMAIN_FIELD)

    st.divider()

    days = st.number_input("Window (days)", min_value=1, max_value=60, value=14)
    gambling_domain = st.text_input("Baseline domain", value=DEFAULT_GAMBLING)

    st.divider()
    st.subheader("🎯 Quality Filters")
    enable_quality_filter = st.checkbox("Enable DR/Traffic filter", value=True,
                                        help="Uncheck to get ALL backlinks regardless of DR/Traffic")
    min_dr = st.number_input("Minimum DR", min_value=0, max_value=100, value=30,
                             disabled=not enable_quality_filter)
    min_traffic = st.number_input("Minimum Traffic", min_value=0, max_value=1000000, value=3000,
                                  disabled=not enable_quality_filter)

    st.divider()
    st.subheader("⚡ Performance")
    max_concurrency = st.slider("Ahrefs concurrency", 2, 30, 12)
    batch_size = st.slider("Batch Analysis size", 10, 100, 100,
                           help="Domains per batch (max 100 per API docs)")
    show_debug = st.checkbox("Show debug info", value=False)

    st.divider()
    st.subheader("🚫 Exclusion Databases")
    st.caption("Domains from selected databases will be excluded.")

    selected_blocklist_dbs = []
    for db in BLOCKLIST_DATABASES:
        if st.checkbox(db['name'], key=f"bl_{db['base_id']}", help=f"Base: {db['base_id']}"):
            selected_blocklist_dbs.append(db)

    st.divider()

    col1, col2 = st.columns(2)
    with col1:
        run_btn = st.button("🚀 Run", type="primary", use_container_width=True)
    with col2:
        refresh_cache_btn = st.button("🔄 Refresh", use_container_width=True)

    clear_cache_btn = st.button("🗑️ Clear Cache", use_container_width=True)
    test_api_btn = st.button("🧪 Test API", use_container_width=True)

# API tokens
AHREFS_TOKEN = os.getenv("AHREFS_API_TOKEN", S.get("AHREFS_API_TOKEN", ""))
AIRTABLE_TOKEN = os.getenv("AIRTABLE_API_KEY", S.get("AIRTABLE_API_KEY", ""))

if clear_cache_btn:
    clear_cache_row(gambling_domain)
    st.success("✅ Cache cleared")

if not AHREFS_TOKEN:
    st.warning("⚠️ Set AHREFS_API_TOKEN in Secrets or environment")
if not AIRTABLE_TOKEN:
    st.warning("⚠️ Set AIRTABLE_API_KEY in Secrets or environment")

shared_session = make_session(pool_size=100)

# ---------------- Test API ----------------
if test_api_btn and AHREFS_TOKEN:
    st.write("## 🧪 API Test")
    ah = AhrefsClient(AHREFS_TOKEN, session=shared_session)

    # Test 0: API Plan Detection — test ahrefs.com (free) vs real domain
    st.write("### Test 0: API Plan Detection")
    with st.spinner("Checking API plan access..."):
        plan_test_results = {}
        for test_target in ["ahrefs.com", "wordcount.com", "moz.com"]:
            try:
                params = {
                    "target": test_target,
                    "mode": "subdomains",
                    "limit": 1,
                    "output": "json",
                    "select": "url_from,first_seen_link",
                }
                response = ah._get(ah.EP_BACKLINKS, params)
                count = len(response.get("backlinks", []))
                plan_test_results[test_target] = f"✅ OK ({count} backlinks)"
            except requests.HTTPError as e:
                error_msg = str(e)
                if "UNAUTHORIZED" in error_msg or "401" in error_msg:
                    plan_test_results[test_target] = "❌ 401 Unauthorized"
                else:
                    plan_test_results[test_target] = f"❌ {error_msg[:80]}"

        for target, result in plan_test_results.items():
            st.write(f"  • `{target}`: {result}")

        # Detect plan type
        free_targets_ok = all("OK" in plan_test_results.get(t, "") for t in ["ahrefs.com", "wordcount.com"])
        real_targets_ok = "OK" in plan_test_results.get("moz.com", "")

        if free_targets_ok and real_targets_ok:
            st.success("✅ Full API access confirmed (Enterprise plan)")
        elif free_targets_ok and not real_targets_ok:
            st.error("❌ API token only works on test targets (ahrefs.com, wordcount.com). "
                     "Your plan does not support querying third-party domains. "
                     "Enterprise plan required for full functionality.")
        else:
            st.error("❌ API token may be invalid — even test targets failed.")

    # Test 1: Raw API call to see response structure
    st.write("### Test 1: Raw Backlinks API Call (ahrefs.com)")
    with st.spinner("Testing raw API call..."):
        try:
            test_target = "ahrefs.com"
            params = {
                "target": test_target,
                "mode": "subdomains",
                "limit": 5,
                "output": "json",
                "select": "url_from,first_seen_link,domain_rating_source",
                "order_by": "first_seen_link:desc",
            }
            st.write(f"📤 Request: GET {ah.BASE}{ah.EP_BACKLINKS}")
            st.write(f"📤 Params: {params}")

            response = ah._get(ah.EP_BACKLINKS, params)
            st.write(f"📥 Response keys: {list(response.keys())}")
            st.json(response)

            # Show how many backlinks were returned
            if "backlinks" in response:
                st.success(f"✅ Found {len(response['backlinks'])} backlinks in response")
            else:
                st.warning("⚠️ No 'backlinks' key in response - check structure above")
        except Exception as e:
            st.error(f"❌ Raw API test failed: {e}")

    # Test 1b: Test with a competitor domain from the list
    st.write("### Test 1b: Test with First Competitor")
    if airtable_base_id and airtable_table and AIRTABLE_TOKEN:
        with st.spinner("Fetching first competitor..."):
            try:
                at_test = AirtableClient(AIRTABLE_TOKEN, airtable_base_id)
                test_comps = at_test.fetch_domains(airtable_table, airtable_domain_field, view_or_id=(airtable_view or None), max_records=3)
                if test_comps:
                    first_comp = test_comps[0]
                    st.write(f"📤 Testing with competitor: **{first_comp}**")

                    # Use AhrefsClient (which has its own session with correct auth)
                    params = {
                        "target": first_comp,
                        "mode": "subdomains",
                        "limit": 10,
                        "output": "json",
                        "select": "url_from,first_seen_link",
                    }
                    st.write(f"📤 Params: {params}")

                    try:
                        response = ah._get(ah.EP_BACKLINKS, params)
                        st.write(f"📥 Response keys: {list(response.keys())}")
                        if "backlinks" in response:
                            st.success(f"✅ Found {len(response['backlinks'])} backlinks for {first_comp}")
                            if response['backlinks']:
                                st.write("First backlink:")
                                st.json(response['backlinks'][0])
                        else:
                            st.json(response)
                    except requests.HTTPError as e:
                        error_msg = str(e)
                        st.error(f"❌ {error_msg[:300]}")
                        if "UNAUTHORIZED" in error_msg or "401" in error_msg:
                            st.warning("💡 Your API token works for test targets (ahrefs.com) "
                                       "but not for third-party domains. This typically means "
                                       "your Ahrefs plan does not include API access for "
                                       "arbitrary domains (Enterprise plan required).")
                else:
                    st.warning("No competitors found in Airtable")
            except Exception as e:
                st.error(f"❌ Competitor test failed: {e}")

    # Test 1c: Raw refdomains — check pagination keys
    st.write("### Test 1c: Refdomains Pagination Check")
    with st.spinner("Testing /refdomains pagination..."):
        try:
            params = {
                "target": "ahrefs.com",
                "mode": "subdomains",
                "limit": 3,
                "output": "json",
                "select": "domain",
            }
            st.write(f"📤 GET {ah.BASE}{ah.EP_REFDOMAINS}")
            st.write(f"📤 Params: {params}")
            response = ah._get(ah.EP_REFDOMAINS, params)
            st.write(f"📥 **All response keys:** `{list(response.keys())}`")
            st.json(response)

            # Test page 2 — if there's an offset or cursor key
            has_cursor = any(k in response for k in ["next", "cursor", "next_cursor", "offset", "has_more"])
            st.write(f"📥 Has pagination token: **{has_cursor}**")
            if has_cursor:
                for k in ["next", "cursor", "next_cursor", "offset", "has_more"]:
                    if k in response:
                        st.write(f"   • `{k}` = `{str(response[k])[:100]}`")

            # Also test with offset param to see if offset-based pagination works
            st.write("---")
            st.write("📤 Testing with `offset=3` for page 2...")
            params2 = params.copy()
            params2["offset"] = 3
            response2 = ah._get(ah.EP_REFDOMAINS, params2)
            st.write(f"📥 Page 2 keys: `{list(response2.keys())}`")
            page2_domains = response2.get("refdomains", [])
            st.write(f"📥 Page 2 returned {len(page2_domains)} domains")
            if page2_domains:
                st.json(page2_domains)
        except Exception as e:
            st.error(f"❌ Refdomains test failed: {e}")

    # Test 2: Backlinks fetch function
    st.write("### Test 2: Backlinks Fetch Function")
    with st.spinner("Testing backlinks fetch..."):
        try:
            test_target = "ahrefs.com"
            test_results = ah.fetch_new_backlinks_raw(test_target, days=30, show_debug=True)
            st.write(f"✅ Backlinks test: {len(test_results)} NEW backlinks for {test_target} (last 30 days)")
            if test_results:
                st.write(f"   First result: {test_results[0]}")
        except Exception as e:
            st.error(f"❌ Backlinks test failed: {e}")

    # Test 3: Batch Analysis endpoint
    st.write("### Test 3: Batch Analysis Endpoint")
    with st.spinner("Testing Batch Analysis API..."):
        try:
            test_domains = ["ahrefs.com", "moz.com", "semrush.com"]
            results = ah.batch_get_domain_metrics(test_domains, batch_size=10, show_debug=True)

            if results:
                auth_errors = sum(1 for v in results.values() if v.get("error") == "unauthorized")
                if auth_errors == 0:
                    st.success(f"✅ Batch Analysis API working!")
                elif auth_errors == len(results):
                    st.error(f"❌ Batch Analysis API returned 401 for all domains. "
                             f"Check API key and plan permissions.")
                else:
                    st.warning(f"⚠️ Batch Analysis: {auth_errors}/{len(results)} domains failed (401)")
                for domain, m in results.items():
                    if m.get("error"):
                        st.write(f"  • {domain}: ❌ {m['error']}")
                    else:
                        dr = m.get('dr', 0)
                        traffic = m.get('traffic', 0)
                        st.write(f"  • {domain}: DR={dr}, Traffic={traffic:,}")
            else:
                st.warning("⚠️ No results returned")
        except Exception as e:
            st.error(f"❌ Batch Analysis test failed: {e}")

# ---------------- Main Pipeline ----------------
def run_pipeline(force_refresh_cache: bool = False):
    """
    OPTIMIZED PIPELINE v3.0 using Batch Analysis API

    Strategy:
    1. Load competitors and blocklists in parallel
    2. Load baseline domains (cached)
    3. Fetch new backlinks for each competitor (parallel)
    4. Collect unique domains not in baseline/blocklist
    5. Use BATCH ANALYSIS API to get DR/Traffic for all domains at once
    6. Apply DR/Traffic filter
    """

    start_time = datetime.now()
    st.write("# 🚀 Competitor Backlinks Analysis v3.0")
    st.info("**Using Batch Analysis API** for efficient DR/Traffic lookup")

    ah = AhrefsClient(AHREFS_TOKEN, session=shared_session)
    at_primary = AirtableClient(AIRTABLE_TOKEN, airtable_base_id)

    # ============================================
    # PHASE 1: Load competitors and blocklists (parallel)
    # ============================================
    st.write("## 📥 Phase 1: Loading Data")

    competitors: List[str] = []
    blocklist_domains: Set[str] = set()
    load_errors: List[str] = []

    def load_competitors() -> List[str]:
        try:
            raw = at_primary.fetch_domains(
                airtable_table,
                airtable_domain_field,
                view_or_id=(airtable_view or None)
            )
            return sorted({sanitize_target_for_ahrefs(x) for x in raw if sanitize_target_for_ahrefs(x)})
        except Exception as e:
            load_errors.append(f"Competitors: {e}")
            return []

    def load_blocklist_db(db_config: Dict[str, str]) -> Set[str]:
        base_id = db_config.get("base_id", "").strip()
        table_id = db_config.get("table_id", BLOCKLIST_TABLE)
        view_id = db_config.get("view_id") or BLOCKLIST_VIEW_ID or None
        db_name = db_config.get("name", base_id)

        if not base_id:
            return set()

        try:
            cli = AirtableClient(AIRTABLE_TOKEN, base_id)
            return set(cli.fetch_domains(table_id, BLOCKLIST_FIELD, view_or_id=view_id))
        except Exception as e:
            load_errors.append(f"{db_name}: {e}")
            return set()

    all_blocklist_configs = selected_blocklist_dbs.copy()
    for base_id in LEGACY_BLOCKLIST_IDS:
        if base_id.strip() and not any(db["base_id"] == base_id.strip() for db in all_blocklist_configs):
            all_blocklist_configs.append({
                "base_id": base_id.strip(),
                "table_id": BLOCKLIST_TABLE,
                "view_id": BLOCKLIST_VIEW_ID or "",
                "name": f"Legacy ({base_id.strip()})"
            })

    with st.spinner("Loading competitors and blocklists..."):
        with ThreadPoolExecutor(max_workers=min(16, 1 + len(all_blocklist_configs))) as pool:
            comp_future = pool.submit(load_competitors)
            blocklist_futures = [pool.submit(load_blocklist_db, db) for db in all_blocklist_configs]

            competitors = comp_future.result()
            for fut in as_completed(blocklist_futures):
                blocklist_domains.update(fut.result() or set())

    st.write(f"✅ **{len(competitors)}** competitors | **{len(blocklist_domains):,}** excluded domains")

    if load_errors:
        with st.expander("⚠️ Load warnings"):
            for err in load_errors:
                st.write(f"- {err}")

    # ============================================
    # PHASE 2: Baseline domains
    # ============================================
    st.write("## 📊 Phase 2: Baseline Domains")

    cache_hit = None if force_refresh_cache else load_ref_domains_from_cache(gambling_domain)
    gdc_ref_domains: List[str] = []

    if cache_hit:
        fetched_at, gdc_ref_domains = cache_hit
        st.success(f"✅ Cache hit: {len(gdc_ref_domains):,} domains (cached {fetched_at.strftime('%Y-%m-%d %H:%M')})")
    else:
        with st.spinner("Fetching baseline domains..."):
            gdc_ref_domains, note = ah.get_baseline_domains(gambling_domain)

            if gdc_ref_domains:
                save_ref_domains_to_cache(gambling_domain, gdc_ref_domains)
                st.success(f"✅ {note}")
            else:
                st.warning("⚠️ Could not fetch baseline domains")

    gdc_ref_set = set(gdc_ref_domains)
    st.write(f"**Baseline:** {len(gdc_ref_set):,} domains for {gambling_domain}")

    # ============================================
    # PHASE 3: Fetch backlinks from competitors
    # ============================================
    st.write("## 🔍 Phase 3: Competitor Backlinks")
    st.info(f"**Window:** Last {days} days | Fetching new backlinks from {len(competitors)} competitors")

    all_backlinks: List[Dict[str, Any]] = []
    api_errors: List[str] = []
    competitor_counts: Dict[str, int] = {}

    def fetch_competitor(comp: str) -> List[Dict[str, Any]]:
        try:
            return ah.fetch_new_backlinks_raw(comp, days=days, show_debug=show_debug)
        except Exception as e:
            api_errors.append(f"{comp}: {str(e)[:100]}")
            return []

    if competitors:
        progress_bar = st.progress(0.0)
        status_text = st.empty()
        completed = 0

        with ThreadPoolExecutor(max_workers=max_concurrency) as pool:
            futures = {pool.submit(fetch_competitor, c): c for c in competitors}

            for future in as_completed(futures):
                comp = futures[future]
                completed += 1

                try:
                    results = future.result() or []
                    competitor_counts[comp] = len(results)

                    for r in results:
                        r["competitor"] = comp
                        all_backlinks.append(r)

                except Exception as e:
                    api_errors.append(f"{comp}: {str(e)[:100]}")

                progress_bar.progress(completed / len(competitors))
                status_text.text(f"Fetching: {completed}/{len(competitors)} | Total backlinks: {len(all_backlinks):,}")

        status_text.empty()

    st.write(f"📊 Fetched **{len(all_backlinks):,}** total backlinks from {len(competitors)} competitors")

    if api_errors:
        with st.expander(f"⚠️ {len(api_errors)} API errors"):
            for err in api_errors[:20]:
                st.write(f"- {err}")

    # ============================================
    # PHASE 4: Filter and collect unique domains
    # ============================================
    st.write("## 🎯 Phase 4: Filtering & Metrics")

    # Collect unique domains not in baseline/blocklist
    unique_domains: Dict[str, Dict[str, Any]] = {}  # domain -> first backlink info
    counters = {"baseline": 0, "blocklist": 0, "duplicate": 0}

    for bl in all_backlinks:
        domain = bl.get("domain", "")
        if not domain:
            continue

        if domain in gdc_ref_set:
            counters["baseline"] += 1
            continue

        if domain in blocklist_domains:
            counters["blocklist"] += 1
            continue

        if domain in unique_domains:
            counters["duplicate"] += 1
            continue

        # New unique domain
        unique_domains[domain] = {
            "source_url": bl.get("source_url", ""),
            "first_seen": bl.get("first_seen", ""),
            "competitor": bl.get("competitor", "")
        }

    st.write(f"📊 **{len(unique_domains):,}** unique domains after filtering")
    st.write(f"   • Filtered (in baseline): {counters['baseline']:,}")
    st.write(f"   • Filtered (in blocklist): {counters['blocklist']:,}")
    st.write(f"   • Duplicates removed: {counters['duplicate']:,}")

    if not unique_domains:
        st.success("No exclusive domains found after filtering.")
        elapsed = (datetime.now() - start_time).total_seconds()
        st.write(f"⏱️ Completed in {elapsed:.1f}s")
        return

    # ============================================
    # PHASE 5: Get DR/Traffic via Batch Analysis API
    # ============================================
    st.write("## 📈 Phase 5: Batch Analysis (DR/Traffic)")
    st.info(f"Getting metrics for {len(unique_domains):,} domains using Batch Analysis API...")

    domain_list = list(unique_domains.keys())
    metrics = ah.batch_get_domain_metrics(domain_list, batch_size=batch_size, show_debug=show_debug)

    # ============================================
    # PHASE 6: Apply DR/Traffic filter and build output
    # ============================================
    st.write("## 📋 Results")

    output_records: List[Dict[str, Any]] = []

    # Check if batch analysis had auth failures
    auth_failures = sum(1 for v in metrics.values() if v.get("error") == "unauthorized")
    if auth_failures > 0:
        st.warning(f"⚠️ {auth_failures} domains returned 401 Unauthorized from Batch Analysis API. "
                   f"DR/Traffic data may be unavailable. Check your Ahrefs API plan.")

    for domain, info in unique_domains.items():
        domain_metrics = metrics.get(domain, {"dr": 0, "traffic": 0})
        dr = domain_metrics.get("dr") or 0
        traffic = domain_metrics.get("traffic") or 0

        # Apply filter if enabled
        if enable_quality_filter:
            if dr < min_dr or traffic < min_traffic:
                continue

        output_records.append({
            "linking_domain": domain,
            "dr": dr,
            "traffic": traffic,
            "source_url": info.get("source_url", ""),
            "first_seen": info.get("first_seen", ""),
            "competitor": info.get("competitor", "")
        })

    elapsed = (datetime.now() - start_time).total_seconds()

    if not output_records:
        if enable_quality_filter:
            st.warning(f"No domains found with DR ≥ {min_dr} and Traffic ≥ {min_traffic:,}")
            st.info("💡 Try disabling the DR/Traffic filter to see all results")
        else:
            st.success("No exclusive domains found after filtering.")
        st.write(f"⏱️ Completed in {elapsed:.1f}s")
        return

    # Create DataFrame
    df = pd.DataFrame.from_records(output_records)
    df = df.sort_values(by=["dr", "traffic"], ascending=[False, False])

    # Metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Unique Domains", f"{len(df):,}")
    with col2:
        st.metric("Avg DR", f"{df['dr'].mean():.1f}")
    with col3:
        st.metric("Avg Traffic", f"{df['traffic'].mean():,.0f}")
    with col4:
        st.metric("Time", f"{elapsed:.1f}s")

    # Table
    st.dataframe(
        df,
        use_container_width=True,
        column_config={
            "linking_domain": st.column_config.TextColumn("Domain", width="medium"),
            "dr": st.column_config.NumberColumn("DR", format="%d"),
            "traffic": st.column_config.NumberColumn("Traffic", format="%,d"),
            "source_url": st.column_config.LinkColumn("Source URL", width="large"),
            "first_seen": st.column_config.TextColumn("First Seen", width="small"),
            "competitor": st.column_config.TextColumn("Competitor", width="medium"),
        }
    )

    # Download
    csv_data = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "📥 Download CSV",
        csv_data,
        f"exclusive_domains_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
        "text/csv",
        use_container_width=True
    )

# ---------------- Action Handlers ----------------
if run_btn:
    if not (airtable_base_id and airtable_table and AHREFS_TOKEN and AIRTABLE_TOKEN):
        st.error("Please configure all required settings")
    else:
        run_pipeline(force_refresh_cache=False)

if refresh_cache_btn:
    if not AHREFS_TOKEN:
        st.error("AHREFS_API_TOKEN required")
    else:
        run_pipeline(force_refresh_cache=True)

# Footer
st.divider()
st.caption("""
**v3.0.6** — Auth fix + Correct endpoints + Honest error reporting
- Fixed: AirtableClient no longer overwrites AhrefsClient auth on shared session
- Fixed: Removed non-existent /new-backlinks and /new-refdomains endpoints (404s)
- Fixed: Uses /all-backlinks with order_by + early-exit date filtering
- Fixed: Batch Analysis 401 errors no longer silently converted to DR=0/Traffic=0
- Fixed: Test panel accurately reports auth failures instead of fake success
- Added: API plan detection (Enterprise vs free-test-only)
- Uses `/batch-analysis` endpoint for DR/Traffic
- Parallel Airtable & backlink fetches
""")
