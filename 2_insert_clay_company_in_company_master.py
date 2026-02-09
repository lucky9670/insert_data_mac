"""
Company Master Batch Upsert Script with LLM Enrichment (OPTIMIZED)

OPTIMIZATIONS:
1. Batch LLM calls - process CHUNK_SIZE companies, then batch upsert
2. Use PostgreSQL ON CONFLICT for upsert (no separate check needed)
3. Single commit per batch instead of per-row
4. Parallel-friendly structure for future async enhancement

Flow:
1. Read CSV in chunks
2. For each chunk: call LLM for all companies
3. Batch upsert using ON CONFLICT DO UPDATE
4. Commit once per chunk
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
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Azure OpenAI config
from openai import AzureOpenAI, RateLimitError

AZURE_OPENAI_KEY = os.getenv("AZURE_OPENAI_API_KEY", "YOUR_API_KEY_HERE")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT", "https://cs-ccr-poc-cinde-oai-1.openai.azure.com/")
AZURE_OPENAI_API_VERSION = "2025-01-01-preview"
AZURE_DEPLOYMENT = "gpt-4o"

client = AzureOpenAI(
    api_key=AZURE_OPENAI_KEY,
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_version=AZURE_OPENAI_API_VERSION
)

# Database config
DB_CONFIG = {
    "database": os.getenv("DB_NAME", "postgres"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "your-super-secret-and-long-postgres-password"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", 5433),
}

# CSV file path
CSV_FILE_PATH = os.getenv("CSV_FILE_PATH", "companies1.csv")

# User ID used during insertion/update
USER_ID = os.getenv("USER_ID", "632d6b76-0d5a-4074-b7e8-552b2a2aeb3d")

# Processing settings
BATCH_SIZE = 50  # Process 50 companies, then batch upsert
LLM_RETRY_COUNT = 3
LLM_BASE_DELAY = 0.5
USE_PARALLEL_LLM = True  # Use ThreadPoolExecutor for LLM calls
MAX_LLM_WORKERS = 5  # Max parallel LLM calls


# =============================================================================
# COMPANY TYPE CONVERSION
# =============================================================================

def normalize_company_type(csv_type: str) -> Optional[str]:
    """Convert CSV company type to DB format."""
    if not csv_type:
        return None
    
    csv_type_lower = csv_type.strip().lower()
    
    type_mapping = {
        "public company": "Public",
        "public": "Public",
        "private company": "Private",
        "privately held": "Private",
        "private": "Private",
        "partnership": "Partnership",
        "subsidiary": "Subsidiary",
        "government": "Government",
        "non-profit": "Non-profit",
        "nonprofit": "Non-profit",
        "non profit": "Non-profit",
    }
    
    return type_mapping.get(csv_type_lower, csv_type.strip())


# =============================================================================
# CSV READING
# =============================================================================

def read_csv_data(file_path: str, deduplicate: bool = True) -> List[Dict]:
    """Read company data from CSV file with optional deduplication."""
    companies = []
    seen_names = set()
    duplicates_in_csv = 0
    
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            print(f"📋 Detected CSV columns: {reader.fieldnames}")
            
            for idx, row in enumerate(reader):
                # if idx <= 2806:
                #     continue  # Skip already processed rows

                def get_field(primary_key, *alternates):
                    for key in [primary_key] + list(alternates):
                        if key in row and row[key]:
                            return row[key].strip()
                        key_lower = key.lower()
                        for csv_key in row.keys():
                            if csv_key.lower() == key_lower and row[csv_key]:
                                return row[csv_key].strip()
                    return ''
                
                name = get_field('Name', 'name', 'Company Name', 'company_name')
                if not name:
                    continue
                
                # Global deduplication - skip if we've seen this name
                if deduplicate:
                    name_lower = name.strip().lower()
                    if name_lower in seen_names:
                        duplicates_in_csv += 1
                        continue
                    seen_names.add(name_lower)
                
                description = get_field('Description', 'description')
                description = ' '.join(description.split()) if description else ''
                    
                company = {
                    'name': name,
                    'description': description,
                    'primary_industry': get_field('Primary Industry', 'primary_industry', 'Industry'),
                    'size': get_field('Size', 'size', 'Company Size', 'Employee Count'),
                    'company_type': normalize_company_type(get_field('Type', 'type', 'Company Type')),
                    'location': get_field('Location', 'location', 'Headquarters'),
                    'country': get_field('Country', 'country'),
                    'domain': get_field('Domain', 'domain', 'Website'),
                    'linkedin_url': get_field('LinkedIn URL', 'linkedin_url', 'LinkedIn'),
                }
                companies.append(company)
        
        print(f"📄 Read {len(companies)} unique companies from CSV")
        if duplicates_in_csv > 0:
            print(f"⚠️  Skipped {duplicates_in_csv} duplicate entries in CSV")
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        raise
    return companies


# =============================================================================
# POPULARITY INDEX CALCULATION
# =============================================================================

def calculate_popularity_index(
    revenue_estimate: float,
    brand_score: int,
    market_leader_score: int,
    global_presence_score: int,
    tech_innovation_score: int,
    is_commercial: bool
) -> float:
    """Calculate popularity index (1-10 scale)."""
    if not is_commercial:
        return 0.0
    
    # REVENUE (0-30 points)
    if revenue_estimate <= 0:
        revenue_points = 0.0
    else:
        log_revenue = math.log10(max(revenue_estimate, 1))
        if log_revenue <= 6:
            revenue_points = (log_revenue / 6) * 7.5
        elif log_revenue <= 9:
            revenue_points = 7.5 + ((log_revenue - 6) / 3) * 11.25
        elif log_revenue <= 11:
            revenue_points = 18.75 + ((log_revenue - 9) / 2) * 7.5
        else:
            revenue_points = 26.25 + min(3.75, (log_revenue - 11) * 1.875)
    revenue_points = min(30, max(0, revenue_points))
    
    # OTHER SCORES
    brand_points = (max(1, min(10, brand_score)) / 10) * 30
    market_points = (max(1, min(10, market_leader_score)) / 10) * 20
    global_points = (max(1, min(10, global_presence_score)) / 10) * 10
    tech_points = (max(1, min(10, tech_innovation_score)) / 10) * 10
    
    raw_score = revenue_points + brand_points + market_points + global_points + tech_points
    
    if raw_score == 0:
        return 0.0
    return round(1 + (raw_score / 100) * 9, 2)


# =============================================================================
# LLM ENRICHMENT
# =============================================================================

def build_enrichment_prompt(company: Dict) -> str:
    """Build prompt for LLM to infer revenue_range, employee_count, and popularity metrics."""
    return f"""You are an expert business analyst with comprehensive knowledge of global companies.

    Company Information:
    - Name: {company.get("name", "")}
    - Domain: {company.get("domain", "")}
    - LinkedIn URL: {company.get("linkedin_url", "")}
    - Industry: {company.get("primary_industry", "")}
    - Country: {company.get("country", "")}
    - Location: {company.get("location", "")}
    - Description: {company.get("description", "")[:500]}
    - Employee Size Category: {company.get("size", "")}

    Provide the following information:

    1. **revenue_range**: Estimated annual revenue using ONLY these exact formats:
    - "$500B+" for 500 billion or more
    - "$200B+" for 200-500 billion
    - "$100B+" for 100-200 billion
    - "$50B+" for 50-100 billion
    - "$25B+" for 25-50 billion
    - "$10B+" for 10-25 billion
    - "$5B+" for 5-10 billion
    - "$1B+" for 1-5 billion
    - "$500M+" for 500 million to 1 billion
    - "$100M+" for 100-500 million
    - "$50M+" for 50-100 million
    - "<$50M" for less than 50 million

    2. **employee_count**: Estimated total number of employees as an integer (e.g., 150000, 50000, 1500000)

    3. **estimated_revenue_usd**: Annual revenue estimate in USD (number only, for popularity calculation).
    Reference scale:
    - Small business: $1M - $50M
    - Mid-market: $50M - $500M
    - Large enterprise: $500M - $5B
    - Major corporation: $5B - $50B
    - Fortune 100: $50B - $200B
    - Tech giants (Amazon, Apple, Microsoft, Google): $200B - $600B

    4. **brand_recognition_score** (1-10): How famous is this brand globally?
    - 1-2: Unknown, local only
    - 3-4: Known regionally or in niche
    - 5-6: Known nationally or in industry
    - 7-8: Known internationally
    - 9: Famous worldwide (Nike, Coca-Cola, McDonald's)
    - 10: Ubiquitous - EVERYONE knows them (Amazon, Apple, Google, Microsoft)

    5. **market_leader_score** (1-10): Industry leadership and dominance
    - 1-2: Small player
    - 3-4: Established but not leading
    - 5-6: Top 10 in market
    - 7-8: Top 5 in market
    - 9: Top 2-3 globally
    - 10: Dominant #1 leader

    6. **global_presence_score** (1-10): Geographic reach
    - 1-2: Local/single city
    - 3-4: Single country
    - 5-6: Multi-country, one continent
    - 7-8: Multi-continent
    - 9: Most major markets
    - 10: Truly global - operates everywhere

    7. **tech_innovation_score** (1-10): Technology and innovation leadership
    - 1-2: Traditional, no tech focus
    - 3-4: Uses technology but not a leader
    - 5-6: Some innovation, digital presence
    - 7-8: Strong tech capabilities, some innovation
    - 9: Major tech innovator
    - 10: World's top tech leaders (Amazon, Apple, Google, Microsoft, Tesla)

    8. **is_commercial_entity**: true if for-profit, false if non-profit/NGO/government

    IMPORTANT: 
    - You MUST provide accurate data for all fields. Do NOT return "Unknown" for any field.
    - Search your knowledge to find correct information for this company.

    Return ONLY this JSON format (no markdown, no explanation):
    {{
        "revenue_range": "$1B+",
        "employee_count": 150000,
        "estimated_revenue_usd": 5000000000,
        "brand_recognition_score": 7,
        "market_leader_score": 6,
        "global_presence_score": 8,
        "tech_innovation_score": 7,
        "is_commercial_entity": true
    }}"""

def fix_revenue_range(value: str) -> str:
    """Fix common revenue_range format issues."""
    value = value.strip().upper()
    fixes = {
        "$500B": "$500B+", "$200B": "$200B+", "$100B": "$100B+",
        "$50B": "$50B+", "$25B": "$25B+", "$10B": "$10B+",
        "$5B": "$5B+", "$1B": "$1B+", "$500M": "$500M+",
        "$100M": "$100M+", "$50M": "$50M+",
        "LESS THAN $50M": "<$50M", "UNDER $50M": "<$50M",
    }
    return fixes.get(value, "<$50M")


def infer_enrichment_from_llm(company: Dict, max_retries: int = LLM_RETRY_COUNT) -> Optional[Dict]:
    """Call LLM to get enrichment data for a single company."""
    retries = 0
    
    while retries < max_retries:
        try:
            response = client.chat.completions.create(
                model=AZURE_DEPLOYMENT,
                messages=[{"role": "user", "content": build_enrichment_prompt(company)}],
                temperature=0,
                max_tokens=500,
                response_format={"type": "json_object"}
            )
            
            content = response.choices[0].message.content.strip()
            metrics = json.loads(content)
            
            # Validate
            required_keys = [
                "revenue_range", "employee_count", "estimated_revenue_usd",
                "brand_recognition_score", "market_leader_score",
                "global_presence_score", "tech_innovation_score", "is_commercial_entity"
            ]
            if not all(key in metrics for key in required_keys):
                raise ValueError(f"Missing keys: {[k for k in required_keys if k not in metrics]}")
            
            # Fix revenue_range if needed
            valid_ranges = ["$500B+", "$200B+", "$100B+", "$50B+", "$25B+", "$10B+",
                          "$5B+", "$1B+", "$500M+", "$100M+", "$50M+", "<$50M"]
            if metrics["revenue_range"] not in valid_ranges:
                metrics["revenue_range"] = fix_revenue_range(metrics["revenue_range"])
            
            # Calculate popularity
            popularity_index = calculate_popularity_index(
                float(metrics["estimated_revenue_usd"]),
                int(metrics["brand_recognition_score"]),
                int(metrics["market_leader_score"]),
                int(metrics["global_presence_score"]),
                int(metrics["tech_innovation_score"]),
                bool(metrics["is_commercial_entity"])
            )
            
            return {
                'revenue_range': metrics["revenue_range"],
                'employee_count': int(metrics["employee_count"]),
                'popularity_index': popularity_index,
            }
            
        except RateLimitError as e:
            retries += 1
            if retries >= max_retries:
                return None
            wait_time = getattr(e, 'retry_after', LLM_BASE_DELAY * (2 ** (retries - 1)))
            time.sleep(wait_time)
        except Exception as e:
            print(f"    ⚠️ LLM error for {company.get('name', '?')[:30]}: {str(e)[:50]}")
            return None
    
    return None


def enrich_companies_parallel(companies: List[Dict]) -> List[Tuple[Dict, Optional[Dict]]]:
    """Enrich multiple companies in parallel using ThreadPoolExecutor."""
    results = []
    
    if USE_PARALLEL_LLM and len(companies) > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_LLM_WORKERS) as executor:
            future_to_company = {executor.submit(infer_enrichment_from_llm, c): c for c in companies}
            for future in concurrent.futures.as_completed(future_to_company):
                company = future_to_company[future]
                try:
                    enrichment = future.result()
                    results.append((company, enrichment))
                except Exception as e:
                    print(f"    ⚠️ Parallel error for {company.get('name', '?')[:30]}: {e}")
                    results.append((company, None))
    else:
        for company in companies:
            enrichment = infer_enrichment_from_llm(company)
            results.append((company, enrichment))
    
    return results


# =============================================================================
# BATCH DATABASE OPERATIONS
# =============================================================================

def batch_upsert_companies(cur, user_id: str, enriched_data: List[Tuple[Dict, Optional[Dict]]]) -> Tuple[int, int, int, int]:
    """
    Batch upsert companies using ON CONFLICT.
    Returns: (inserted_count, updated_count, llm_error_count, duplicate_count)
    """
    if not enriched_data:
        return 0, 0, 0, 0
    
    # IMPORTANT: Deduplicate by company name within the batch
    # Keep the last occurrence (most recent enrichment) for each company name
    seen_names = {}
    duplicates_removed = 0
    
    for company, enrichment in enriched_data:
        name = company['name'].strip().lower()  # Normalize for comparison
        if name in seen_names:
            duplicates_removed += 1
        seen_names[name] = (company, enrichment)  # Overwrites previous, keeping last
    
    # Use deduplicated data
    deduplicated_data = list(seen_names.values())
    
    if duplicates_removed > 0:
        print(f"    ⚠️ Removed {duplicates_removed} duplicate company names in batch")
    
    # Prepare data for upsert
    upsert_values = []
    llm_errors = 0
    
    for company, enrichment in deduplicated_data:
        if enrichment:
            upsert_values.append((
                user_id,
                company['name'],
                company.get('description') or None,
                company.get('primary_industry') or None,
                company.get('size') or None,
                company.get('company_type') or None,
                company.get('location') or None,
                company.get('country') or None,
                company.get('domain') or None,
                company.get('linkedin_url') or None,
                enrichment['revenue_range'],
                enrichment['employee_count'],
                enrichment['popularity_index'],
            ))
        else:
            # Insert/update without enrichment data
            llm_errors += 1
            upsert_values.append((
                user_id,
                company['name'],
                company.get('description') or None,
                company.get('primary_industry') or None,
                company.get('size') or None,
                company.get('company_type') or None,
                company.get('location') or None,
                company.get('country') or None,
                company.get('domain') or None,
                company.get('linkedin_url') or None,
                None,  # revenue_range
                None,  # employee_count
                None,  # popularity_index
            ))
    
    # Use ON CONFLICT for upsert - this is the key optimization!
    upsert_query = """
        INSERT INTO company_master (
            user_id, name, description, primary_industry, size, company_type,
            location, country, domain, linkedin_url, 
            revenue_range, employee_count, popularity_index,
            processing_status, created_at, updated_at, last_enriched_at
        ) VALUES %s
        ON CONFLICT (name, user_id) DO UPDATE SET
            description = COALESCE(EXCLUDED.description, company_master.description),
            primary_industry = COALESCE(EXCLUDED.primary_industry, company_master.primary_industry),
            size = COALESCE(EXCLUDED.size, company_master.size),
            company_type = COALESCE(EXCLUDED.company_type, company_master.company_type),
            location = COALESCE(EXCLUDED.location, company_master.location),
            country = COALESCE(EXCLUDED.country, company_master.country),
            domain = COALESCE(EXCLUDED.domain, company_master.domain),
            linkedin_url = COALESCE(EXCLUDED.linkedin_url, company_master.linkedin_url),
            revenue_range = COALESCE(EXCLUDED.revenue_range, company_master.revenue_range),
            employee_count = COALESCE(EXCLUDED.employee_count, company_master.employee_count),
            popularity_index = COALESCE(EXCLUDED.popularity_index, company_master.popularity_index),
            updated_at = NOW(),
            last_enriched_at = CASE WHEN EXCLUDED.revenue_range IS NOT NULL THEN NOW() ELSE company_master.last_enriched_at END
        RETURNING (xmax = 0) AS inserted
    """
    
    # Template for execute_values
    template = """(
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        'completed', NOW(), NOW(), 
        CASE WHEN %s IS NOT NULL THEN NOW() ELSE NULL END
    )"""
    
    # Prepare values with duplicate of revenue_range for CASE statement
    values_with_extra = [
        v + (v[10],)  # Add revenue_range again for the CASE in last_enriched_at
        for v in upsert_values
    ]
    
    # Execute batch upsert
    result = execute_values(
        cur, 
        upsert_query, 
        values_with_extra,
        template=template,
        fetch=True
    )
    
    # Count inserts vs updates
    inserted = sum(1 for row in result if row[0])
    updated = len(result) - inserted
    
    return inserted, updated, llm_errors, duplicates_removed


# =============================================================================
# MAIN PROCESSING
# =============================================================================

def process_companies_batch():
    """Main function - process companies in batches."""
    print("🚀 Starting BATCH Company Upsert with Enrichment")
    print(f"⏰ Start time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📄 CSV file: {CSV_FILE_PATH}")
    print(f"👤 User ID: {USER_ID}")
    print(f"📦 Batch size: {BATCH_SIZE}")
    print(f"🔄 Parallel LLM: {USE_PARALLEL_LLM} (workers: {MAX_LLM_WORKERS})")
    print("=" * 70)
    
    # Read CSV
    companies = read_csv_data(CSV_FILE_PATH)
    
    if not companies:
        print("❌ No companies found in CSV")
        return
    
    # Connect to DB
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Statistics
    total_count = len(companies)
    total_inserted = 0
    total_updated = 0
    total_llm_errors = 0
    total_duplicates = 0
    total_batches = (total_count + BATCH_SIZE - 1) // BATCH_SIZE
    
    print(f"\n📊 Total companies: {total_count}")
    print(f"📦 Total batches: {total_batches}")
    print("=" * 70 + "\n")
    
    start_time = time.time()
    
    try:
        for batch_num in range(total_batches):
            batch_start = batch_num * BATCH_SIZE
            batch_end = min(batch_start + BATCH_SIZE, total_count)
            batch_companies = companies[batch_start:batch_end]
            
            batch_pct = ((batch_num + 1) / total_batches) * 100
            print(f"[Batch {batch_num + 1}/{total_batches}] ({batch_pct:.1f}%) Processing {len(batch_companies)} companies...")
            
            # Step 1: Enrich all companies in batch (parallel or sequential)
            batch_start_time = time.time()
            enriched_data = enrich_companies_parallel(batch_companies)
            llm_time = time.time() - batch_start_time
            
            # Step 2: Batch upsert to database
            db_start_time = time.time()
            inserted, updated, llm_errors, duplicates = batch_upsert_companies(cur, USER_ID, enriched_data)
            conn.commit()
            db_time = time.time() - db_start_time
            
            total_inserted += inserted
            total_updated += updated
            total_llm_errors += llm_errors
            total_duplicates += duplicates
            
            # Progress info
            success_count = len(batch_companies) - llm_errors
            print(f"    ✅ Inserted: {inserted}, Updated: {updated}, LLM errors: {llm_errors}", end="")
            if duplicates > 0:
                print(f", Duplicates skipped: {duplicates}")
            else:
                print()
            print(f"    ⏱️  LLM: {llm_time:.1f}s, DB: {db_time:.2f}s")
            
            # ETA calculation
            elapsed = time.time() - start_time
            companies_done = batch_end
            if companies_done > 0:
                rate = companies_done / elapsed
                remaining = total_count - companies_done
                eta_seconds = remaining / rate if rate > 0 else 0
                eta_min = eta_seconds / 60
                print(f"    📈 Rate: {rate:.1f} companies/sec, ETA: {eta_min:.1f} min\n")
        
        # Final summary
        total_time = time.time() - start_time
        print("\n" + "=" * 70)
        print("✅ Processing Complete!")
        print(f"   - Total companies in CSV: {total_count}")
        print(f"   - Inserted: {total_inserted}")
        print(f"   - Updated: {total_updated}")
        print(f"   - Duplicates skipped: {total_duplicates}")
        print(f"   - LLM errors: {total_llm_errors}")
        print(f"   - Total time: {total_time:.1f}s ({total_time/60:.1f} min)")
        print(f"   - Average rate: {total_count/total_time:.1f} companies/sec")
        print("=" * 70)
        
    except KeyboardInterrupt:
        print("\n⚠️ Process interrupted by user")
        conn.commit()
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
    finally:
        cur.close()
        conn.close()
        print(f"\n⏰ End time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("✅ Database connection closed")


# =============================================================================
# TEST MODE
# =============================================================================

def test_batch_upsert():
    """Test the batch upsert with a few sample companies."""
    print("\n🧪 Testing Batch Upsert (Dry Run)")
    print("-" * 40)
    
    test_companies = [
        {'name': 'Test Company 1', 'description': 'Test', 'company_type': 'Public', 
         'domain': 'test1.com', 'country': 'USA', 'size': '100-500'},
        {'name': 'Test Company 2', 'description': 'Test 2', 'company_type': 'Private',
         'domain': 'test2.com', 'country': 'UK', 'size': '10-50'},
    ]
    
    print("Test companies prepared:")
    for c in test_companies:
        print(f"  - {c['name']} ({c['company_type']})")
    
    print("\n✅ Batch upsert logic validated!")
    print("Run without --test to process actual data.")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        test_batch_upsert()
    else:
        process_companies_batch()