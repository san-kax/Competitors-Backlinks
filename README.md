# Competitor NEW backlinks vs Gambling.com (Airtable + Ahrefs)

This Streamlit app pulls competitor domains from a **primary Airtable**, fetches their **last N days** new backlinks from **Ahrefs**, compares against **all referring domains to Gambling.com**, and excludes any domain present in **four additional Airtable blocklists**. Final output lists domains competitors gained that **Gambling.com doesn't have** and **aren't in your blocklists**.

## Inputs
- Primary Airtable table containing:
  - `Domain` (text; bare domain or full URL — normalized automatically)
  - `Sheet Name (Geo)` (optional; currently not used in comparison but can be added back easily)
- Blocklist Airtable bases (4 bases in your links), each with a table containing a `Domain` field.

## Quick start (local)
```bash
python -m venv .venv && source .venv/bin/activate  # windows: .venv\Scripts\activate
pip install -r requirements.txt
# Provide secrets via env or .streamlit/secrets.toml
export AHREFS_API_TOKEN="..."
export AIRTABLE_API_KEY="..."
streamlit run app.py
```

## Deploy to Streamlit Cloud
1. Push this repo to GitHub.
2. In Streamlit Cloud, create a new app pointing to `app.py`.
3. **Secrets** (App → Settings → Secrets): paste something like:
```toml
AHREFS_API_TOKEN = "ahrefs_v3_token"
AIRTABLE_API_KEY = "pat_xxx"

# Primary Airtable
AIRTABLE_BASE_ID = "appDEgCV6C4vLGjEY"
AIRTABLE_TABLE = "Sheet1"
AIRTABLE_VIEW = "Grid view"
AIRTABLE_DOMAIN_FIELD = "Domain"
AIRTABLE_GEO_FIELD = "Sheet Name (Geo)"
GAMBLING_DOMAIN = "gambling.com"

# Blocklists (your four additional bases)
BLOCKLIST_BASE_IDS = "appUoOvkqzJvyyMvC,appHdhjsWVRxaCvcR,appVyIiM5boVyoBhf,appJTJQwjHRaAyLkw"
BLOCKLIST_TABLE_NAME = "Domains"
BLOCKLIST_DOMAIN_FIELD = "Domain"
```
4. Deploy. Use the sidebar to tweak days, domain fields, or base IDs.

## How it works
- **Airtable → competitors**: paginated API calls pull `Domain`; normalized to **registrable domains** using `tldextract` (handles `.co.uk`, `.bet.br`, etc.).
- **Ahrefs → new backlinks**: For each competitor, the app fetches backlinks with `first_seen >= today-N days`. From each backlink we extract `source_url → linking domain`.
- **Gambling.com cache**: The app fetches **referring domains** for `gambling.com` once and stores them in `backlink_cache.sqlite`. Use **Refresh Gambling.com cache** to refetch.
- **Blocklists**: Merges domain sets from the listed Airtable bases, and removes them from results.
- **Compare**: Report domains that newly linked to competitors, but **do not** link to Gambling.com and **do not** appear in blocklists.

## Notes / Tips
- If your Ahrefs plan uses different path/params, adjust `AhrefsClient.ENDPOINT_*` and the `params` dicts.
- If you need Geo back in the output, we can add it alongside per-domain mapping from the primary table.
- The CSV export includes: `competitor, linking_domain, source_url, first_seen`.
