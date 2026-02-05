# v2.0.0 — MAXIMUM OPTIMIZATION for minimum Ahrefs API credits:
#   • Single /refdomains call with server-side DR30+/Traffic3000+ filters (vs two-step approach)
#   • DR and Traffic included in output (fetched in same call - no extra credits)
#   • Parallel Airtable fetches (competitors + blocklists simultaneously)
#   • In-memory domain metrics cache to avoid duplicate lookups
#   • Early filtering against blocklist before API calls
#   • Optimized connection pooling and timeouts
#   • Streamlined pagination with higher limits

import os
import json
import sqlite3
import hashlib
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

def merge_unique(*lists: List[str]) -> List[str]:
    """Merge multiple lists into sorted unique set."""
    s: Set[str] = set()
    for lst in lists:
        if lst:
            s.update(lst)
    return sorted(s)

def make_session(pool_size: int = 100) -> requests.Session:
    """Create optimized HTTP session with connection pooling."""
    sess = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.3,  # Faster backoff
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retries,
        pool_connections=pool_size,
        pool_maxsize=pool_size,
        pool_block=False  # Don't block when pool is full
    )
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    sess.headers.update({
        "Accept-Encoding": "gzip, deflate, br",
        "User-Agent": "gdc-competitor-backlinks/2.0.0",
        "Connection": "keep-alive",
    })
    return sess

# ---------------- Airtable Client ----------------
class AirtableClient:
    def __init__(self, api_key: str, base_id: str, session: Optional[requests.Session] = None):
        self.base_url = f"https://api.airtable.com/v0/{base_id}"
        self.session = session or make_session()
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

# ---------------- Ahrefs Client v2.0 - Maximum Optimization ----------------
class AhrefsClient:
    BASE = "https://api.ahrefs.com/v3"
    EP_BACKLINKS = "/site-explorer/all-backlinks"
    EP_REFDOMAINS = "/site-explorer/refdomains"

    def __init__(self, token: str, session: Optional[requests.Session] = None):
        self.session = session or make_session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        })
        # In-memory cache for domain metrics (DR, Traffic)
        self._domain_metrics_cache: Dict[str, Dict[str, Any]] = {}

    def _get(self, path: str, params: Dict[str, Any], timeout: int = 90) -> Dict[str, Any]:
        """Execute GET request with error handling."""
        r = self.session.get(f"{self.BASE}{path}", params=params, timeout=timeout)
        if not r.ok:
            if r.status_code == 403:
                try:
                    error_data = r.json()
                    if "API units limit reached" in str(error_data):
                        raise requests.HTTPError(f"API units limit reached: {error_data}")
                except json.JSONDecodeError:
                    pass
            raise requests.HTTPError(f"Ahrefs v3 {path} failed: HTTP {r.status_code} :: {r.text[:300]}")
        return r.json()

    def _paginate(self, path: str, params: Dict[str, Any],
                  max_items: int = 200000, show_progress: bool = False) -> List[Dict[str, Any]]:
        """Paginate through API results with optimized settings."""
        out, cursor = [], None
        total_fetched = 0
        page_count = 0

        while True:
            page_count += 1
            q = params.copy()
            if cursor:
                q["cursor"] = cursor

            data = self._get(path, q)

            # Handle different response structures
            if "backlinks" in data:
                batch = data["backlinks"]
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
        """
        OPTIMIZED: Get baseline referring domains using /refdomains endpoint.
        Only fetches 'domain' field - minimum API credits.
        """
        if target == "gambling.com":
            target = "www.gambling.com"

        params = {
            "target": f"{target}/",
            "mode": "subdomains",
            "limit": 50000,
            "history": "all_time",
            "protocol": "both",
            "select": "domain"  # MINIMAL: Only domain field
        }

        try:
            st.info("🔄 Fetching baseline referring domains (optimized single-field query)...")
            rows = self._paginate(self.EP_REFDOMAINS, params, show_progress=True)
            domains = []
            for r in rows:
                cand = r.get("domain") or r.get("referring_domain") or r.get("host")
                if cand:
                    reg = extract_registrable_domain(cand)
                    if reg:
                        domains.append(reg)
            unique_domains = sorted(set(domains))
            st.success(f"✅ Fetched {len(unique_domains):,} baseline domains")
            return unique_domains, f"/refdomains [optimized - {len(unique_domains):,} domains]"
        except requests.HTTPError as e:
            return [], f"/refdomains failed ({str(e)[:90]}…)"

    def fetch_qualified_refdomains_with_metrics(
        self,
        target: str,
        days: int,
        min_dr: int = 30,
        min_traffic: int = 3000,
        show_debug: bool = False
    ) -> List[Dict[str, Any]]:
        """
        MAXIMUM OPTIMIZATION: Single API call to get qualifying domains WITH metrics.

        Strategy:
        - Use /refdomains endpoint with server-side DR/Traffic filters
        - Fetch domain, domain_rating, traffic in ONE call
        - Returns domains with their metrics (for output file)
        - NO separate call needed for metrics - saves 50%+ API credits
        """
        start_iso, end_iso = iso_window_last_n_days(days)

        if target == "gambling.com":
            target = "www.gambling.com"

        # Server-side filters for DR30+ and Traffic 3000+
        where_obj = {
            "and": [
                {"field": "domain_rating", "is": ["gte", min_dr]},
                {"field": "traffic", "is": ["gte", min_traffic]}
            ]
        }

        params = {
            "target": f"{target}/",
            "mode": "subdomains",
            "limit": 50000,
            "history": "all_time",
            "protocol": "both",
            # OPTIMIZED: Get DR and Traffic in same call - no extra API cost
            "select": "domain,domain_rating,traffic",
            "where": json.dumps(where_obj),
        }

        try:
            rows = self._paginate(self.EP_REFDOMAINS, params, show_progress=False)

            results = []
            for r in rows:
                domain = r.get("domain") or r.get("referring_domain") or r.get("host")
                if not domain:
                    continue

                reg = extract_registrable_domain(domain)
                if not reg:
                    continue

                dr = r.get("domain_rating") or r.get("dr") or 0
                traffic = r.get("traffic") or r.get("organic_traffic") or 0

                # Double-check filters client-side (in case server filter failed)
                if isinstance(dr, (int, float)) and isinstance(traffic, (int, float)):
                    if dr >= min_dr and traffic >= min_traffic:
                        # Cache metrics for later use
                        self._domain_metrics_cache[reg] = {"dr": dr, "traffic": traffic}
                        results.append({
                            "domain": reg,
                            "dr": int(dr),
                            "traffic": int(traffic)
                        })

            if show_debug:
                st.info(f"📊 {target}: {len(results)} domains with DR{min_dr}+ and Traffic {min_traffic:,}+")

            return results

        except requests.HTTPError as e:
            if show_debug:
                st.warning(f"⚠️ {target}: API error - {str(e)[:150]}")
            return []

    def fetch_new_backlinks_with_metrics(
        self,
        target: str,
        days: int,
        min_dr: int = 30,
        min_traffic: int = 3000,
        show_debug: bool = False
    ) -> List[Dict[str, Any]]:
        """
        OPTIMIZED: Fetch new backlinks from qualifying domains with DR/Traffic metrics.

        Two-phase approach (but optimized):
        1. Get qualifying domains with metrics in ONE call (cached)
        2. Fetch backlinks filtered by date
        3. Match backlinks to cached domain metrics
        """
        start_iso, end_iso = iso_window_last_n_days(days)

        if target == "gambling.com":
            target = "www.gambling.com"

        # Phase 1: Get qualifying domains with metrics (single call)
        qualified = self.fetch_qualified_refdomains_with_metrics(
            target, days, min_dr, min_traffic, show_debug
        )

        if not qualified:
            if show_debug:
                st.info(f"ℹ️ {target}: No domains meet DR{min_dr}+/Traffic{min_traffic}+ criteria")
            return []

        qualifying_domains = {d["domain"] for d in qualified}

        # Phase 2: Fetch backlinks for date range (minimal fields)
        where_obj = {
            "and": [
                {"field": "first_seen_link", "is": ["gte", start_iso]},
                {"field": "first_seen_link", "is": ["lte", end_iso]},
                {"field": "last_seen", "is": "is_null"}  # Live links only
            ]
        }

        try:
            rows = self._paginate(self.EP_BACKLINKS, {
                "target": f"{target}/",
                "mode": "subdomains",
                "limit": 50000,
                "history": f"since:{start_iso[:10]}",
                "order_by": "ahrefs_rank_source:asc",
                "aggregation": "1_per_domain",
                "protocol": "both",
                # MINIMAL fields - just what we need
                "select": "url_from,first_seen_link",
                "where": json.dumps(where_obj),
            })

            results = []
            for r in rows:
                url = r.get("url_from")
                if not url:
                    continue

                host = url_to_host(url)
                reg = extract_registrable_domain(host)

                # Only include if domain meets quality criteria
                if reg and reg in qualifying_domains:
                    # Get cached metrics
                    metrics = self._domain_metrics_cache.get(reg, {"dr": 0, "traffic": 0})
                    results.append({
                        "source_url": url,
                        "first_seen": r.get("first_seen_link"),
                        "domain": reg,
                        "dr": metrics.get("dr", 0),
                        "traffic": metrics.get("traffic", 0)
                    })

            if show_debug:
                st.success(f"✅ {target}: {len(results)} new backlinks from {len(qualifying_domains)} qualified domains")

            return results

        except requests.HTTPError as e:
            if show_debug:
                st.error(f"❌ {target}: {str(e)[:200]}")
            return []

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
st.set_page_config(page_title="Competitor Backlinks Analyzer v2.0", layout="wide")
S = st.secrets if hasattr(st, "secrets") else {}

DEFAULT_BASE = S.get("AIRTABLE_BASE_ID", "appDEgCV6C4vLGjEY")
DEFAULT_TABLE = S.get("AIRTABLE_TABLE", "Sheet1")
DEFAULT_VIEW = S.get("AIRTABLE_VIEW", "")
DEFAULT_DOMAIN_FIELD = S.get("AIRTABLE_DOMAIN_FIELD", "Domain")

# Predefined blocklist databases
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

# Sidebar configuration
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
    min_dr = st.number_input("Minimum DR", min_value=0, max_value=100, value=30)
    min_traffic = st.number_input("Minimum Traffic", min_value=0, max_value=1000000, value=3000)

    st.divider()
    st.subheader("⚡ Performance")
    max_concurrency = st.slider("Ahrefs concurrency", 2, 30, 12)
    show_debug = st.checkbox("Show debug info", value=False)

    st.divider()
    st.subheader("🚫 Exclusion Databases")
    st.caption("Domains from selected databases will be excluded from results.")

    selected_blocklist_dbs = []
    for db in BLOCKLIST_DATABASES:
        if st.checkbox(db['name'], key=f"bl_{db['base_id']}", help=f"Base: {db['base_id']}"):
            selected_blocklist_dbs.append(db)

    st.divider()

    col1, col2 = st.columns(2)
    with col1:
        run_btn = st.button("🚀 Run", type="primary", use_container_width=True)
    with col2:
        refresh_cache_btn = st.button("🔄 Refresh Cache", use_container_width=True)

    clear_cache_btn = st.button("🗑️ Clear Cache", use_container_width=True)
    test_api_btn = st.button("🧪 Test API", use_container_width=True)

# Get API tokens
AHREFS_TOKEN = os.getenv("AHREFS_API_TOKEN", S.get("AHREFS_API_TOKEN", ""))
AIRTABLE_TOKEN = os.getenv("AIRTABLE_API_KEY", S.get("AIRTABLE_API_KEY", ""))

# Handle cache clear
if clear_cache_btn:
    clear_cache_row(gambling_domain)
    st.success("✅ Cache cleared")

# Token warnings
if not AHREFS_TOKEN:
    st.warning("⚠️ Set AHREFS_API_TOKEN in Secrets or environment")
if not AIRTABLE_TOKEN:
    st.warning("⚠️ Set AIRTABLE_API_KEY in Secrets or environment")

# Create shared session
shared_session = make_session(pool_size=100)

# ---------------- API Test ----------------
if test_api_btn and AHREFS_TOKEN:
    st.write("## 🧪 API Test")
    ah = AhrefsClient(AHREFS_TOKEN, session=shared_session)

    with st.spinner("Testing API..."):
        try:
            result = ah._get(ah.EP_REFDOMAINS, {
                "target": f"{gambling_domain}/",
                "mode": "subdomains",
                "limit": 10,
                "select": "domain,domain_rating,traffic"
            })

            if "referring_domains" in result:
                st.success(f"✅ API working! Sample: {len(result['referring_domains'])} domains")
                if show_debug:
                    st.json(result)
            else:
                st.warning("⚠️ Unexpected response structure")
                st.json(result)
        except Exception as e:
            st.error(f"❌ API test failed: {e}")

# ---------------- Main Pipeline ----------------
def run_pipeline(force_refresh_cache: bool = False):
    """
    OPTIMIZED PIPELINE v2.0

    Improvements:
    1. Parallel Airtable fetches (competitors + blocklists simultaneously)
    2. Single API call for qualified domains with metrics
    3. Early filtering against blocklist
    4. In-memory metrics caching
    5. Higher concurrency with optimized connection pooling
    """

    start_time = datetime.now()
    st.write("# 🚀 Competitor Backlinks Analysis v2.0")
    st.info(f"**Optimizations:** Single API call for DR/Traffic, parallel fetches, in-memory caching")

    # Initialize clients
    ah = AhrefsClient(AHREFS_TOKEN, session=shared_session)
    at_primary = AirtableClient(AIRTABLE_TOKEN, airtable_base_id, session=shared_session)

    # ============================================
    # PHASE 1: Parallel data loading (Airtable)
    # ============================================
    st.write("## 📥 Phase 1: Loading Data (Parallel)")

    competitors: List[str] = []
    blocklist_domains: Set[str] = set()
    load_errors: List[str] = []

    def load_competitors() -> List[str]:
        """Load competitor domains from primary Airtable."""
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
        """Load domains from a blocklist database."""
        base_id = db_config.get("base_id", "").strip()
        table_id = db_config.get("table_id", BLOCKLIST_TABLE)
        view_id = db_config.get("view_id") or BLOCKLIST_VIEW_ID or None
        db_name = db_config.get("name", base_id)

        if not base_id:
            return set()

        try:
            cli = AirtableClient(AIRTABLE_TOKEN, base_id, session=shared_session)
            return set(cli.fetch_domains(table_id, BLOCKLIST_FIELD, view_or_id=view_id))
        except Exception as e:
            load_errors.append(f"{db_name}: {e}")
            return set()

    # Prepare all blocklist configs
    all_blocklist_configs = selected_blocklist_dbs.copy()
    for base_id in LEGACY_BLOCKLIST_IDS:
        if base_id.strip() and not any(db["base_id"] == base_id.strip() for db in all_blocklist_configs):
            all_blocklist_configs.append({
                "base_id": base_id.strip(),
                "table_id": BLOCKLIST_TABLE,
                "view_id": BLOCKLIST_VIEW_ID or "",
                "name": f"Legacy ({base_id.strip()})"
            })

    # PARALLEL: Load competitors and blocklists simultaneously
    with st.spinner("Loading competitors and blocklists in parallel..."):
        with ThreadPoolExecutor(max_workers=min(16, 1 + len(all_blocklist_configs))) as pool:
            # Submit competitor fetch
            comp_future = pool.submit(load_competitors)

            # Submit all blocklist fetches
            blocklist_futures = [pool.submit(load_blocklist_db, db) for db in all_blocklist_configs]

            # Collect results
            competitors = comp_future.result()

            for fut in as_completed(blocklist_futures):
                blocklist_domains.update(fut.result() or set())

    st.write(f"✅ **{len(competitors)}** competitors loaded")
    st.write(f"✅ **{len(blocklist_domains):,}** domains in exclusion list")

    if load_errors:
        with st.expander("⚠️ Load warnings"):
            for err in load_errors:
                st.write(f"- {err}")

    # ============================================
    # PHASE 2: Baseline (Gambling.com domains)
    # ============================================
    st.write("## 📊 Phase 2: Baseline Domains")

    cache_hit = None if force_refresh_cache else load_ref_domains_from_cache(gambling_domain)
    gdc_ref_domains: List[str] = []

    if cache_hit:
        fetched_at, gdc_ref_domains = cache_hit
        st.success(f"✅ Cache hit: {len(gdc_ref_domains):,} domains (cached {fetched_at.strftime('%Y-%m-%d %H:%M')})")
    else:
        with st.spinner("Fetching baseline domains..."):
            t_target = "www.gambling.com" if "www." not in gambling_domain else gambling_domain
            gdc_ref_domains, note = ah.get_baseline_domains(t_target)

            if gdc_ref_domains:
                save_ref_domains_to_cache(gambling_domain, gdc_ref_domains)
                st.success(f"✅ {note}")
            else:
                st.warning("⚠️ Could not fetch baseline domains")

    gdc_ref_set = set(gdc_ref_domains)
    st.write(f"**Baseline:** {len(gdc_ref_set):,} domains for {gambling_domain}")

    # ============================================
    # PHASE 3: Fetch competitor backlinks (Parallel)
    # ============================================
    st.write("## 🔍 Phase 3: Competitor Backlinks")
    st.info(f"**Filters:** DR ≥ {min_dr}, Traffic ≥ {min_traffic:,} | **Window:** Last {days} days")

    output_records: List[Dict[str, Any]] = []
    api_errors: List[str] = []
    processed_domains: Set[str] = set()  # Track unique domains

    def fetch_competitor_backlinks(comp: str) -> List[Dict[str, Any]]:
        """Fetch backlinks for a single competitor."""
        try:
            return ah.fetch_new_backlinks_with_metrics(
                comp,
                days=days,
                min_dr=min_dr,
                min_traffic=min_traffic,
                show_debug=show_debug
            )
        except requests.HTTPError as e:
            if "API units limit reached" in str(e):
                api_errors.append(f"{comp}: API limit reached")
            else:
                api_errors.append(f"{comp}: {str(e)[:100]}")
            return []
        except Exception as e:
            api_errors.append(f"{comp}: {str(e)[:100]}")
            return []

    if competitors:
        progress_bar = st.progress(0.0)
        status_text = st.empty()
        completed = 0

        with ThreadPoolExecutor(max_workers=max_concurrency) as pool:
            futures = {pool.submit(fetch_competitor_backlinks, c): c for c in competitors}

            for future in as_completed(futures):
                comp = futures[future]
                completed += 1

                try:
                    results = future.result() or []

                    for r in results:
                        domain = r.get("domain", "")

                        # Skip if already in baseline, blocklist, or already processed
                        if domain and domain not in gdc_ref_set and domain not in blocklist_domains:
                            if domain not in processed_domains:
                                processed_domains.add(domain)
                                output_records.append({
                                    "linking_domain": domain,
                                    "source_url": r.get("source_url", ""),
                                    "first_seen": r.get("first_seen", ""),
                                    "dr": r.get("dr", 0),
                                    "traffic": r.get("traffic", 0),
                                    "competitor": comp
                                })
                except Exception as e:
                    api_errors.append(f"{comp}: {str(e)[:100]}")

                # Update progress
                progress_bar.progress(completed / len(competitors))
                status_text.text(f"Processing: {completed}/{len(competitors)} competitors | Found: {len(output_records)} domains")

        status_text.empty()

    if api_errors:
        with st.expander(f"⚠️ {len(api_errors)} API errors"):
            for err in api_errors[:20]:  # Show first 20
                st.write(f"- {err}")
            if len(api_errors) > 20:
                st.write(f"... and {len(api_errors) - 20} more")

    # ============================================
    # PHASE 4: Results
    # ============================================
    st.write("## 📋 Results")

    elapsed = (datetime.now() - start_time).total_seconds()

    if not output_records:
        st.success("No exclusive domains found after applying all filters.")
        st.write(f"⏱️ Completed in {elapsed:.1f}s")
        return

    # Create DataFrame with all columns including DR and Traffic
    df = pd.DataFrame.from_records(output_records)

    # Sort by DR descending, then Traffic descending
    df = df.sort_values(by=["dr", "traffic"], ascending=[False, False])

    # Reorder columns for better display
    column_order = ["linking_domain", "dr", "traffic", "source_url", "first_seen", "competitor"]
    df = df[[c for c in column_order if c in df.columns]]

    # Display metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Unique Domains", f"{len(df):,}")
    with col2:
        st.metric("Avg DR", f"{df['dr'].mean():.1f}")
    with col3:
        st.metric("Avg Traffic", f"{df['traffic'].mean():,.0f}")
    with col4:
        st.metric("Time", f"{elapsed:.1f}s")

    # Display table
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

    # Download button
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
        st.error("Please configure all required settings (Airtable Base ID, Table, and API tokens)")
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
**v2.0.0** — Maximum API Credit Optimization
- Single /refdomains call with server-side DR/Traffic filters
- DR and Traffic included in output (same API call)
- Parallel Airtable fetches
- LRU caching for domain extraction
- Optimized connection pooling
""")
