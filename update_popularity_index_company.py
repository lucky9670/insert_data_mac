import psycopg2
import os
import time
import math
import json
import csv
from openai import AzureOpenAI, RateLimitError
from psycopg2.extras import execute_batch
from typing import Dict, List, Optional

# Azure OpenAI config
AZURE_OPENAI_KEY = os.getenv("AZURE_OPENAI_API_KEY", "4QIwqZy4sqKTfpnlDgG0EE8FqeaHfXEfZS8CvR4ApuoaykbatNbJJQQJ99BKACYeBjFXJ3w3AAABACOGa3KY")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT", "https://cs-ccr-poc-cinde-oai-1.openai.azure.com/")
AZURE_OPENAI_API_VERSION = "2025-01-01-preview"
AZURE_DEPLOYMENT = "gpt-4o"

client = AzureOpenAI(
    api_key=AZURE_OPENAI_KEY,
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_version=AZURE_OPENAI_API_VERSION
)

# Database config
# DB_CONFIG = {
#     "host": "db.rhgkggssbehwhocqsmwv.supabase.co",
#     "port": "5432",
#     "dbname": "postgres",
#     "user": "postgres",
#     "password": "Noida305901"
# }

DB_CONFIG = {
    "host": "db.efenamzdehiszyccpxnn.supabase.co",
    "port": "5432",
    "dbname": "postgres",
    "user": "postgres",
    "password": "EDEDFQWp3i7MlX8z"
}

# CSV file path
CSV_FILE_PATH = "output/prod_enriched_companies_300.csv"

# User ID used during insertion
USER_ID = "632d6b76-0d5a-4074-b7e8-552b2a2aeb3d"

CHUNK_SIZE = 100


def read_csv_data(file_path: str) -> List[Dict]:
    """Read all company data from CSV file as-is"""
    companies = []
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                company = {
                    'name': row.get('name', '').strip(),
                    'domain': row.get('domain', '').strip(),
                    'linkedin_url': row.get('linkedin_url', '').strip(),
                    'headquarters': row.get('headquarters', '').strip(),
                    'location': row.get('location', '').strip(),
                    'description': row.get('description', '').strip(),
                    'primary_industry': row.get('primary_industry', '').strip(),
                    'size': row.get('size', '').strip(),
                    'revenue_range': row.get('revenue_range', '').strip(),
                    'country': row.get('country', '').strip(),
                }
                if company['name']:
                    companies.append(company)
        print(f"📄 Read {len(companies)} companies from CSV")
    except FileNotFoundError:
        print(f"❌ CSV file not found: {file_path}")
        raise
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        raise
    return companies


def get_company_id(cur, user_id: str, name: str) -> Optional[str]:
    """Check if company exists in DB and return its ID, otherwise return None"""
    query = "SELECT id FROM company_master WHERE user_id = %s AND name = %s"
    cur.execute(query, (user_id, name))
    result = cur.fetchone()
    return result[0] if result else None


def calculate_popularity_index(revenue_estimate: float, brand_score: int, is_commercial: bool) -> float:
    """Calculate popularity index based on revenue, brand score, and commercial status"""
    if not is_commercial:
        return 0.0
    
    if revenue_estimate <= 0:
        revenue_index = 1.0
    else:
        log_revenue = math.log10(revenue_estimate)
        revenue_index = max(1.0, min(10.0, (log_revenue - 6) * 2.25 + 1))
    
    brand_score = max(1, min(10, brand_score))
    popularity_index = (0.80 * revenue_index) + (0.20 * brand_score)
    
    return round(popularity_index, 2)


def build_popularity_only_prompt(company: Dict) -> str:
    """Build prompt for popularity metrics estimation"""
    return f"""You are a company popularity analyst. Analyze the company and provide popularity metrics ONLY.

        Company Information:
        - Name: {company.get("name", "")}
        - Domain: {company.get("domain", "")}
        - LinkedIn URL: {company.get("linkedin_url", "")}
        - Industry: {company.get("primary_industry", "")}
        - Country: {company.get("country", "")}
        - Location: {company.get("location", "")}
        - Description: {company.get("description", "")}
        - Size: {company.get("size", "")}
        - Revenue Range: {company.get("revenue_range", "")}

        Task: Provide popularity metrics for calculating popularity_index:

        1. **estimated_revenue_usd**: Best estimate of annual revenue in USD (number only, e.g., 5000000 for $5M). Estimate based on company size, industry, and any available data. Use 0 if truly impossible to estimate.

        2. **brand_recognition_score**: Rate brand/global recognition from 1-10 where:
        * 1-2: Unknown/local company
        * 3-4: Regional presence
        * 5-6: National presence
        * 7-8: International presence
        * 9-10: Global household name

        3. **is_commercial_entity**: true if for-profit business, false if non-profit/association/user group/community organization

        Rules:
        - Be realistic with revenue estimates based on company size
        - Brand score: most companies are 3-6, only famous brands get 8+
        - Return ONLY valid JSON, no additional text

        Return in this EXACT JSON format:
        {{
            "estimated_revenue_usd": 5000000,
            "brand_recognition_score": 5,
            "is_commercial_entity": true
        }}
    """


def infer_popularity_from_llm(company: Dict, max_retries: int = 3):
    """Call LLM to get popularity metrics"""
    retries = 0
    base_delay = 0.5
    
    while retries < max_retries:
        try:
            response = client.chat.completions.create(
                model=AZURE_DEPLOYMENT,
                messages=[{"role": "user", "content": build_popularity_only_prompt(company)}],
                temperature=0,
                max_tokens=150,
                response_format={"type": "json_object"}
            )
            
            content = response.choices[0].message.content.strip()
            metrics = json.loads(content)
            
            # Validate response
            required_keys = ["estimated_revenue_usd", "brand_recognition_score", "is_commercial_entity"]
            if not all(key in metrics for key in required_keys):
                raise ValueError(f"Missing required keys in response: {metrics}")
            
            # Calculate popularity index
            popularity_index = calculate_popularity_index(
                float(metrics["estimated_revenue_usd"]),
                int(metrics["brand_recognition_score"]),
                bool(metrics["is_commercial_entity"])
            )
            
            return popularity_index
            
        except RateLimitError as e:
            retries += 1
            if retries >= max_retries:
                print(f"❌ Max retries for {company.get('name', '?')}")
                return None
            
            retry_after = getattr(e, 'retry_after', None)
            wait_time = retry_after if retry_after else base_delay * (2 ** (retries - 1))
            
            print(f"⏳ Rate limit. Waiting {wait_time:.1f}s (retry {retries}/{max_retries})")
            time.sleep(wait_time)
        except json.JSONDecodeError as e:
            print(f"❌ JSON parsing error for {company.get('name', '?')}: {str(e)[:100]}")
            return None
        except Exception as e:
            print(f"❌ Error for {company.get('name', '?')}: {str(e)[:100]}")
            return None
    
    return None


def bulk_update_popularity(cur, conn, updates: List[tuple]):
    """Bulk update popularity_index by id"""
    query = "UPDATE company_master SET popularity_index = %s WHERE id = %s"
    execute_batch(cur, query, updates, page_size=100)
    conn.commit()


def main():
    print("🚀 Starting Popularity Index Update for CSV Companies")
    print(f"⏰ Start time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📄 CSV file: {CSV_FILE_PATH}")
    print(f"👤 User ID: {USER_ID}")
    
    # Step 1: Read all CSV data as-is
    companies = read_csv_data(CSV_FILE_PATH)
    
    if not companies:
        print("❌ No companies found in CSV")
        return
    
    # Step 2: Connect to database
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    total_count = len(companies)
    total_updated = 0
    total_skipped_not_in_db = 0
    total_skipped_llm_error = 0
    updates = []  # List to collect updates: (popularity_index, id)
    
    print(f"\n{'='*60}")
    print(f"📊 Total companies in CSV: {total_count}")
    print(f"{'='*60}\n")

    try:
        for idx, company in enumerate(companies):
            progress = idx + 1
            pct = (progress / total_count) * 100
            
            # Step 3: Check if company exists in DB
            company_id = get_company_id(cur, USER_ID, company['name'])
            
            if company_id is None:
                # Company not in DB - skip, no LLM call
                total_skipped_not_in_db += 1
                print(f"[{progress}/{total_count}] ({pct:.1f}%) ⏭️ SKIP (not in DB): {company['name'][:50]}")
                continue
            
            # Step 4: Company exists - call LLM
            # popularity_index = infer_popularity_from_llm(company)
            popularity_index = 5.0
            
            if popularity_index is not None:
                # Add to updates list: (popularity_index, id)
                updates.append((popularity_index, company_id))
                total_updated += 1
                print(f"[{progress}/{total_count}] ({pct:.1f}%) ✅ {company['name'][:50]}: {popularity_index}")
            else:
                total_skipped_llm_error += 1
                print(f"[{progress}/{total_count}] ({pct:.1f}%) ⚠️ LLM ERROR: {company['name'][:50]}")
            
            # Step 5: When list reaches chunk size, bulk update and clear
            if len(updates) >= CHUNK_SIZE:
                print(f"\n📦 Bulk updating {len(updates)} records...")
                bulk_update_popularity(cur, conn, updates)
                print(f"✅ Committed {len(updates)} updates\n")
                updates = []  # Clear the list
                time.sleep(0.3)
        
        # Step 6: Update remaining records
        if updates:
            print(f"\n📦 Bulk updating remaining {len(updates)} records...")
            bulk_update_popularity(cur, conn, updates)
            print(f"✅ Committed {len(updates)} updates")
        
        print(f"\n{'='*60}")
        print(f"✅ Processing Complete!")
        print(f"   - Total in CSV: {total_count}")
        print(f"   - Updated: {total_updated}")
        print(f"   - Skipped (not in DB): {total_skipped_not_in_db}")
        print(f"   - Skipped (LLM error): {total_skipped_llm_error}")
        print(f"{'='*60}")
        
    except KeyboardInterrupt:
        print("\n⚠️ Process interrupted by user")
        if updates:
            print(f"💾 Saving {len(updates)} pending updates before exit...")
            bulk_update_popularity(cur, conn, updates)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cur.close()
        conn.close()
        print(f"\n⏰ End time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("✅ Database connection closed")


if __name__ == "__main__":
    main()