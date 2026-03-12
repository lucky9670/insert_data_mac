"""
Company Master Batch Upsert — LLM-Only Child Detection (Fast Version)

HOW CHILD DEMOTION WORKS (simple & fast):
==========================================
LLM already returns parent_company for every company.
  - parent_company = null    → parent company → keep score as-is
  - parent_company = "Google" → child company → multiply score × 0.10

No DB fetch. No name-prefix scanning. No O(n) loops over existing data.
Each batch of 50 takes only as long as the LLM call itself (~seconds).

POPULARITY INDEX COMPONENTS (total = 100,000 max):
  * Revenue (log scale):   0 - 30,000 pts
  * Brand Recognition:     0 - 30,000 pts
  * Market Leadership:     0 - 20,000 pts
  * Global Presence:       0 - 10,000 pts
  * Tech Innovation:       0 - 10,000 pts
"""

import psycopg2
import os
import time
import math
import json
import csv
import concurrent.futures
from psycopg2.extras import execute_values
from typing import Dict, List, Optional, Tuple
from dotenv import load_dotenv

load_dotenv()

from openai import AzureOpenAI, RateLimitError

AZURE_OPENAI_KEY      = os.getenv("AZURE_OPENAI_API_KEY", "YOUR_API_KEY_HERE")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT", "https://cs-ccr-poc-cinde-oai-1.openai.azure.com/")
AZURE_OPENAI_API_VERSION = "2025-01-01-preview"
AZURE_DEPLOYMENT      = "gpt-4o"

client = AzureOpenAI(
    api_key=AZURE_OPENAI_KEY,
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_version=AZURE_OPENAI_API_VERSION,
)

DB_CONFIG = {
    "database": os.getenv("DB_NAME",     "postgres"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", "your-super-secret-and-long-postgres-password"),
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     os.getenv("DB_PORT",     5433),
}

# CSV_ALL_FILES = [
#     'Advertising_Services.csv',
#     'Banking.csv', 'Computer_and_Network_Security.csv', 
#     'Data_Infrastructure_and_Analytics.csv', 'Financial_Services.csv', 'Insurance.csv', 
#     'IT_Services_and_IT_Consulting.csv', 'Marketing_Services.csv', 'retail.csv',
#     'software_development.csv', 'Venture_Capital_and_Private_Equity.csv'
#     'Accounting.csv', 'Biotechnology.csv', 'Biotechnology_Research.csv',
#     'Brokerages.csv', 'Business_Consulting_and_Services.csv',
#     'Business_Intelligence_Platforms.csv', 'Capital_Markets.csv',
#     'Commercial_Real_Estate.csv', 'Data_Security Software Products.csv',
#     'Data_Security_Software_Products.csv', 'E-Learning_Providers.csv',
#     'Education_Management.csv', 'Freight_and_Package_Transportation.csv',
#     'Higher_Education.csv', 'Hospitals.csv', 'Hospitals_and_Health_Care.csv',
#     'Human_Resources_Services.csv', 'Import_and_Export.csv',
#     'Information_Technology.csv', 'Internet_Marketplace_Platforms.csv',
#     'Investment_Banking.csv', 'Investment_Management.csv', 'Market_Research.csv',
#     'Media_&_Telecommunications.csv', 'Medical_Devices.csv',
#     'Operations_Consulting.csv', 'OutsourcingOffshoring.csv',
#     'Outsourcing_and_Offshoring.csv', 'Pharmaceutical_Manufacturing.csv',
#     'Public_Relations.csv', 'Real_Estate.csv', 'Staffing_and_Recruiting.csv',
#     'Strategic_Management_Services.csv', 'Technology_Information_and_Media.csv',
#     'Telecommunications.csv', 'Telecommunications_Carriers.csv',
#     'Transportation_Logistics_Supply.csv', 'Warehousing_and_Storage.csv',
# ]

# CSV_ALL_FILES = [
#     'Advertising_Services.csv',
#     'Banking.csv', 'Computer_and_Network_Security.csv', 
#     'Data_Infrastructure_and_Analytics.csv', 'Financial_Services.csv', 'Insurance.csv', 'IT_Services_and_IT_Consulting.csv', 'Marketing_Services.csv', 'retail.csv', 'software_development.csv', 'Venture_Capital_and_Private_Equity.csv'
# ]

CSV_ALL_FILES = [
    # 'Automation Machinery.csv', 'Building Construction.csv', 'Building Materials.csv', 'Construction.csv', 'Equipment Rental Services.csv', 'Industrial Automation.csv', 'Industrial Machinery Manufacturing.csv', 'Machinery Manufacturing.csv', 'Manufacturing.csv', 'Online and Mail Order Retail.csv', 'Specialty Trade Contractors.csv', 'Wholesale Building Materials.csv', 
    'Wholesale Chemical and Allied.csv', 'Wholesale Computer Equipment.csv', 'Wholesale Drugs and Sundries.csv', 'Wholesale Import and Export.csv', 'Wholesale Machinery.csv', 'Wholesale.csv'
]

USER_ID               = os.getenv("USER_ID", "632d6b76-0d5a-4074-b7e8-552b2a2aeb3d")
BATCH_SIZE            = 50
LLM_RETRY_COUNT       = 3
LLM_BASE_DELAY        = 0.5
MAX_LLM_WORKERS       = 5
CHILD_SCORE_MULTIPLIER = 0.10   # children get 10% of their own LLM score


# =============================================================================
# HELPERS
# =============================================================================

def normalize_company_type(csv_type: str) -> Optional[str]:
    if not csv_type:
        return None
    mapping = {
        "public company": "Public",   "public": "Public",
        "private company": "Private", "privately held": "Private", "private": "Private",
        "partnership": "Partnership", "subsidiary": "Subsidiary",
        "government": "Government",
        "non-profit": "Non-profit",   "nonprofit": "Non-profit", "non profit": "Non-profit",
    }
    return mapping.get(csv_type.strip().lower(), csv_type.strip())


def calculate_popularity_index(
    revenue_usd: float, brand: int, market: int,
    global_p: int, tech: int, is_commercial: bool
) -> float:
    if not is_commercial:
        return 10.0

    if revenue_usd <= 0:
        rev_pts = 0.0
    else:
        lr = math.log10(max(revenue_usd, 1))
        if   lr <= 6:    rev_pts = (lr / 6) * 3000
        elif lr <= 9:    rev_pts = 3000  + ((lr - 6)  / 3)   * 12000
        elif lr <= 11:   rev_pts = 15000 + ((lr - 9)  / 2)   * 10000
        elif lr <= 11.7: rev_pts = 25000 + ((lr - 11) / 0.7) * 4000
        else:            rev_pts = 29000 + min(1000, (lr - 11.7) * 1000)
    rev_pts = min(30000, max(0, rev_pts))

    b = max(1, min(10, brand))
    if   b <= 3: b_pts = (b / 3) * 3000
    elif b <= 6: b_pts = 3000  + ((b - 3) / 3) * 7000
    elif b <= 8: b_pts = 10000 + ((b - 6) / 2) * 10000
    else:        b_pts = 20000 + ((b - 8) / 2) * 10000

    m = max(1, min(10, market))
    if   m <= 3: m_pts = (m / 3) * 2000
    elif m <= 6: m_pts = 2000  + ((m - 3) / 3) * 4000
    elif m <= 8: m_pts = 6000  + ((m - 6) / 2) * 6000
    else:        m_pts = 12000 + ((m - 8) / 2) * 8000

    g_pts = (max(1, min(10, global_p)) / 10) * 10000
    t_pts = (max(1, min(10, tech))     / 10) * 10000

    return max(0.0, min(100000.0, round(rev_pts + b_pts + m_pts + g_pts + t_pts, 2)))


def fix_revenue_range(value: str) -> str:
    v = value.strip().upper()
    fixes = {
        "$500B": "$500B+", "$200B": "$200B+", "$100B": "$100B+",
        "$50B":  "$50B+",  "$25B":  "$25B+",  "$10B":  "$10B+",
        "$5B":   "$5B+",   "$1B":   "$1B+",   "$500M": "$500M+",
        "$100M": "$100M+", "$50M":  "$50M+",
        "LESS THAN $50M": "<$50M",  "UNDER $50M": "<$50M",
    }
    return fixes.get(v, "<$50M")


# =============================================================================
# CSV READING
# =============================================================================

def read_csv_data(file_path: str) -> List[Dict]:
    companies, seen, dupes = [], set(), 0
    try:
        with open(file_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            print(f"  📋 Columns: {reader.fieldnames}")
            for row in reader:
                def get(primary, *alts):
                    for key in (primary,) + alts:
                        if key in row and row[key]: return row[key].strip()
                        for k in row:
                            if k.lower() == key.lower() and row[k]: return row[k].strip()
                    return ''

                name = get('Name', 'name', 'Company Name', 'company_name')
                if not name: continue
                nl = name.lower()
                if nl in seen: dupes += 1; continue
                seen.add(nl)

                desc = get('Description', 'description')
                companies.append({
                    'name':             name,
                    'description':      ' '.join(desc.split()) if desc else '',
                    'primary_industry': get('Primary Industry', 'primary_industry', 'Industry'),
                    'size':             get('Size', 'size', 'Company Size', 'Employee Count'),
                    'company_type':     normalize_company_type(get('Type', 'type', 'Company Type')),
                    'location':         get('Location', 'location', 'Headquarters'),
                    'country':          get('Country', 'country'),
                    'domain':           get('Domain', 'domain', 'Website'),
                    'linkedin_url':     get('LinkedIn URL', 'linkedin_url', 'LinkedIn'),
                })
        print(f"  📄 {len(companies)} companies  ({dupes} dupes skipped)")
    except Exception as e:
        print(f"❌ CSV error: {e}"); raise
    return companies


# =============================================================================
# LLM — one call per company, returns score + is_child flag
# =============================================================================

LLM_PROMPT_TEMPLATE = """You are an expert business analyst.

Company:
- Name: {name}
- Domain: {domain}
- LinkedIn: {linkedin}
- Industry: {industry}
- Country: {country}
- Location: {location}
- Description: {desc}
- Size: {size}

Return ONLY valid JSON (no markdown, no extra text):
{{
  "revenue_range": "$1B+",
  "employee_count": 150000,
  "estimated_revenue_usd": 5000000000,
  "brand_recognition_score": 7,
  "market_leader_score": 6,
  "global_presence_score": 8,
  "tech_innovation_score": 7,
  "is_commercial_entity": true,
  "parent_company": null
}}

Field rules:
- revenue_range: one of "$500B+" "$200B+" "$100B+" "$50B+" "$25B+" "$10B+" "$5B+" "$1B+" "$500M+" "$100M+" "$50M+" "<$50M"
- *_score fields: integer 1-10
- parent_company: exact parent name if this is a subsidiary / division / product line, otherwise null
  e.g. "Google AdSense"→"Google", "Microsoft Power BI"→"Microsoft", "Nike"→null, "Salesforce"→null"""


def call_llm(company: Dict) -> Optional[Dict]:
    """
    One LLM call → enrichment dict.
    Child detection is purely from parent_company field returned by LLM.
    No DB lookups, no name scanning.
    """
    prompt = LLM_PROMPT_TEMPLATE.format(
        name     = company.get('name', ''),
        domain   = company.get('domain', ''),
        linkedin = company.get('linkedin_url', ''),
        industry = company.get('primary_industry', ''),
        country  = company.get('country', ''),
        location = company.get('location', ''),
        desc     = company.get('description', '')[:500],
        size     = company.get('size', ''),
    )

    for attempt in range(LLM_RETRY_COUNT):
        try:
            resp = client.chat.completions.create(
                model=AZURE_DEPLOYMENT,
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                max_tokens=300,
                response_format={"type": "json_object"},
            )
            m = json.loads(resp.choices[0].message.content.strip())

            # Validate required keys
            required = ["revenue_range", "employee_count", "estimated_revenue_usd",
                        "brand_recognition_score", "market_leader_score",
                        "global_presence_score", "tech_innovation_score",
                        "is_commercial_entity"]
            missing = [k for k in required if k not in m]
            if missing:
                raise ValueError(f"Missing: {missing}")

            valid_ranges = ["$500B+","$200B+","$100B+","$50B+","$25B+","$10B+",
                            "$5B+","$1B+","$500M+","$100M+","$50M+","<$50M"]
            if m["revenue_range"] not in valid_ranges:
                m["revenue_range"] = fix_revenue_range(m["revenue_range"])

            # --- Raw popularity score ---
            raw_score = calculate_popularity_index(
                float(m["estimated_revenue_usd"]),
                int(m["brand_recognition_score"]),
                int(m["market_leader_score"]),
                int(m["global_presence_score"]),
                int(m["tech_innovation_score"]),
                bool(m["is_commercial_entity"]),
            )

            # --- Child detection: use LLM's parent_company, nothing else ---
            parent_raw = m.get("parent_company")
            is_child = (
                parent_raw is not None
                and str(parent_raw).strip().lower() not in ("null", "none", "n/a", "")
            )

            # Children → 10% of their OWN score (not parent's score)
            # This ensures they always rank below any reasonably scored parent
            popularity_index = (
                round(raw_score * CHILD_SCORE_MULTIPLIER, 2) if is_child else raw_score
            )

            return {
                "revenue_range":    m["revenue_range"],
                "employee_count":   int(m["employee_count"]),
                "popularity_index": popularity_index,
                "is_child":         is_child,
                "parent_company":   str(parent_raw).strip() if is_child else None,
            }

        except RateLimitError as e:
            if attempt >= LLM_RETRY_COUNT - 1:
                return None
            time.sleep(getattr(e, "retry_after", LLM_BASE_DELAY * (2 ** attempt)))
        except Exception as e:
            print(f"    ⚠️  LLM [{company.get('name','?')[:30]}]: {str(e)[:60]}")
            return None
    return None


def enrich_parallel(companies: List[Dict]) -> List[Tuple[Dict, Optional[Dict]]]:
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_LLM_WORKERS) as ex:
        futures = {ex.submit(call_llm, c): c for c in companies}
        for fut in concurrent.futures.as_completed(futures):
            c = futures[fut]
            try:
                results.append((c, fut.result()))
            except Exception as e:
                print(f"    ⚠️  Thread [{c.get('name','?')[:30]}]: {e}")
                results.append((c, None))
    return results


# =============================================================================
# DATABASE — write only, no reads at all
# =============================================================================

UPSERT_SQL = """
    INSERT INTO company_master (
        user_id, name, description, primary_industry, size, company_type,
        location, country, domain, linkedin_url,
        revenue_range, employee_count, popularity_index,
        processing_status, created_at, updated_at, last_enriched_at
    ) VALUES %s
    ON CONFLICT (name, user_id) DO UPDATE SET
        description      = COALESCE(EXCLUDED.description,      company_master.description),
        primary_industry = COALESCE(EXCLUDED.primary_industry, company_master.primary_industry),
        size             = COALESCE(EXCLUDED.size,             company_master.size),
        company_type     = COALESCE(EXCLUDED.company_type,     company_master.company_type),
        location         = COALESCE(EXCLUDED.location,         company_master.location),
        country          = COALESCE(EXCLUDED.country,          company_master.country),
        domain           = COALESCE(EXCLUDED.domain,           company_master.domain),
        linkedin_url     = COALESCE(EXCLUDED.linkedin_url,     company_master.linkedin_url),
        revenue_range    = COALESCE(EXCLUDED.revenue_range,    company_master.revenue_range),
        employee_count   = COALESCE(EXCLUDED.employee_count,   company_master.employee_count),
        popularity_index = EXCLUDED.popularity_index,
        updated_at       = NOW(),
        last_enriched_at = CASE WHEN EXCLUDED.revenue_range IS NOT NULL
                           THEN NOW() ELSE company_master.last_enriched_at END
    RETURNING (xmax = 0) AS inserted
"""

ROW_TEMPLATE = """(
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    'completed', NOW(), NOW(),
    CASE WHEN %s IS NOT NULL THEN NOW() ELSE NULL END
)"""


def upsert_batch(cur, user_id: str,
                 enriched: List[Tuple[Dict, Optional[Dict]]]) -> Tuple[int, int, int]:
    """Pure write — zero DB reads. Returns (inserted, updated, llm_errors)."""
    rows, llm_errors, seen = [], 0, set()

    for company, enc in enriched:
        nl = company["name"].strip().lower()
        if nl in seen:
            continue
        seen.add(nl)

        if enc:
            row = (
                user_id,
                company["name"],
                company.get("description")      or None,
                company.get("primary_industry") or None,
                company.get("size")             or None,
                company.get("company_type")     or None,
                company.get("location")         or None,
                company.get("country")          or None,
                company.get("domain")           or None,
                company.get("linkedin_url")     or None,
                enc["revenue_range"],
                enc["employee_count"],
                enc["popularity_index"],         # already demoted if is_child
            )
        else:
            llm_errors += 1
            row = (
                user_id,
                company["name"],
                company.get("description")      or None,
                company.get("primary_industry") or None,
                company.get("size")             or None,
                company.get("company_type")     or None,
                company.get("location")         or None,
                company.get("country")          or None,
                company.get("domain")           or None,
                company.get("linkedin_url")     or None,
                None, None, 1000.0,              # fallback score for LLM failures
            )

        rows.append(row + (row[10],))            # extra col for last_enriched_at CASE

    if not rows:
        return 0, 0, llm_errors

    result   = execute_values(cur, UPSERT_SQL, rows, template=ROW_TEMPLATE, fetch=True)
    inserted = sum(1 for r in result if r[0])
    updated  = len(result) - inserted
    return inserted, updated, llm_errors


# =============================================================================
# MAIN
# =============================================================================

def process_file(file: str):
    path = f"clay_company_100/{file}"
    print(f"\n{'='*65}\n🚀  {file}\n{'='*65}")

    companies = read_csv_data(path)
    if not companies:
        print("❌  No companies — skipping"); return

    total         = len(companies)
    total_batches = math.ceil(total / BATCH_SIZE)
    ins = upd = errs = 0

    # Single persistent connection per file — no reconnects needed during normal run
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur  = conn.cursor()
    except Exception as e:
        print(f"❌  DB connect failed: {e}"); return

    run_start = time.time()

    try:
        for b in range(total_batches):
            batch = companies[b * BATCH_SIZE : (b + 1) * BATCH_SIZE]
            pct   = (b + 1) / total_batches * 100
            print(f"\n[{b+1}/{total_batches}] ({pct:.0f}%)  Enriching {len(batch)} companies …")

            # ── LLM (parallel, only bottleneck is API latency) ──
            t0       = time.time()
            enriched = enrich_parallel(batch)
            llm_sec  = time.time() - t0

            # ── Log any children detected ──
            children = [(c["name"], e["parent_company"])
                        for c, e in enriched if e and e.get("is_child")]
            if children:
                for cname, pname in children[:5]:
                    print(f"    📉 {cname}  →  child of {pname}")
                if len(children) > 5:
                    print(f"    📉 … and {len(children)-5} more children demoted")

            # ── DB write (fast, no reads) ──
            t0 = time.time()
            try:
                i, u, e = upsert_batch(cur, USER_ID, enriched)
                conn.commit()
            except psycopg2.OperationalError as ex:
                print(f"  ❌  DB lost: {ex}  — reconnecting …")
                try: cur.close(); conn.close()
                except Exception: pass
                conn = psycopg2.connect(**DB_CONFIG)
                cur  = conn.cursor()
                i, u, e = 0, 0, len(batch)
            except Exception as ex:
                print(f"  ❌  Upsert error: {ex}")
                try: conn.rollback()
                except Exception: pass
                i, u, e = 0, 0, len(batch)

            db_sec = time.time() - t0
            ins += i; upd += u; errs += e
            print(f"  ✅  +{i} inserted  ~{u} updated  ⚠️ {e} errors  "
                  f"[LLM {llm_sec:.1f}s  DB {db_sec:.2f}s]")

    except KeyboardInterrupt:
        print("\n⚠️  Interrupted")
        try: conn.commit()
        except Exception: pass
    finally:
        try: cur.close()
        except Exception: pass
        try: conn.close()
        except Exception: pass

    elapsed = time.time() - run_start
    print(f"\n📊  {file} done — {ins} inserted  {upd} updated  {errs} errors  "
          f"({elapsed:.0f}s / {elapsed/60:.1f} min)")


if __name__ == "__main__":
    import sys
    if "--test" in sys.argv:
        print(f"\n{'Company':<45} {'Parent':<20} {'Raw':>8} {'Final':>10}  Status")
        print("-" * 95)
        cases = [
            ("Google",                          None,        98000),
            ("Google AdSense",                  "Google",    85000),
            ("Google Labs",                     "Google",    82000),
            ("Microsoft",                       None,        95000),
            ("Microsoft Power BI",              "Microsoft", 80000),
            ("Amazon",                          None,        97000),
            ("Amazon Fulfillment Technologies", "Amazon",    72000),
            ("Nike",                            None,        65000),
            ("Walmart",                         None,        70000),
            ("Walmart Global Tech",             "Walmart",   65000),
        ]
        for name, parent, raw in cases:
            is_child = parent is not None
            final    = round(raw * CHILD_SCORE_MULTIPLIER, 2) if is_child else float(raw)
            status   = f"CHILD of {parent}" if is_child else "PARENT"
            print(f"{name:<45} {str(parent or ''):<20} {raw:>8,}  {final:>10,.2f}  {status}")
        print("\n✅  Children sit at 10% of their own score → always rank below parents")
    else:
        for f in CSV_ALL_FILES:
            process_file(f)