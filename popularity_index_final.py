import psycopg2
import os
import time
import math
import json
import csv
from openai import AzureOpenAI, RateLimitError
from psycopg2.extras import execute_batch
from typing import Dict, List, Optional, Tuple

# Azure OpenAI config - USE ENVIRONMENT VARIABLES IN PRODUCTION
AZURE_OPENAI_KEY = os.getenv("AZURE_OPENAI_API_KEY", "4QIwqZy4sqKTfpnlDgG0EE8FqeaHfXEfZS8CvR4ApuoaykbatNbJJQQJ99BKACYeBjFXJ3w3AAABACOGa3KY")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT", "https://cs-ccr-poc-cinde-oai-1.openai.azure.com/")
AZURE_OPENAI_API_VERSION = "2025-01-01-preview"
AZURE_DEPLOYMENT = "gpt-4o"

client = AzureOpenAI(
    api_key=AZURE_OPENAI_KEY,
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_version=AZURE_OPENAI_API_VERSION
)

# Database config - USE ENVIRONMENT VARIABLES IN PRODUCTION
# DB_CONFIG = {
#     "host": "db.rhgkggssbehwhocqsmwv.supabase.co",
#     "port": "5432",
#     "dbname": "postgres",
#     "user": "postgres",
#     "password": "Noida305901"
# }

DB_CONFIG = {
    "database": "postgres",
    "user": "postgres",
    "password": "your-super-secret-and-long-postgres-password",
    "host": "localhost",
    "port": 5433,
    "minconn": 2,
    "maxconn": 10
}

# CSV file path
CSV_FILE_PATH = "output/prod_enriched_companies_300.csv"

# User ID used during insertion
USER_ID = "632d6b76-0d5a-4074-b7e8-552b2a2aeb3d"

CHUNK_SIZE = 100


def read_csv_data(file_path: str) -> List[Dict]:
    """
    Read company data from CSV file.
    NOTE: Revenue data from CSV is IGNORED - LLM estimates all metrics.
    """
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
                    'revenue_range': row.get('revenue_range', '').strip(),  # Read but ignored
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


def calculate_popularity_index(
    revenue_estimate: float,
    brand_score: int,
    market_leader_score: int,
    global_presence_score: int,
    tech_innovation_score: int,
    is_commercial: bool
) -> float:
    """
    Calculate popularity index with adjusted weighting to prioritize tech giants.
    
    ADJUSTED WEIGHTING (to put Amazon, Apple, Google, Microsoft at top):
    - Revenue: 30 points (reduced from 40) - still important but not dominant
    - Brand Recognition: 30 points (increased from 25) - household names rank higher
    - Market Leadership: 20 points (same) - industry dominance matters
    - Global Presence: 10 points (reduced from 15) - widespread but less weighted
    - Tech/Innovation: 10 points (NEW) - tech leaders get a boost
    
    Total: 100 points, normalized to 1-10 scale
    
    Args:
        revenue_estimate: Annual revenue in USD (from LLM)
        brand_score: Brand recognition 1-10
        market_leader_score: Industry leadership 1-10
        global_presence_score: Global reach 1-10
        tech_innovation_score: Tech/innovation leadership 1-10
        is_commercial: True if for-profit entity
    
    Returns:
        Popularity index from 1-10 (or 0 for non-commercial)
    """
    if not is_commercial:
        return 0.0
    
    # =========================================================================
    # REVENUE COMPONENT (0-30 points) - Reduced from 40
    # =========================================================================
    if revenue_estimate <= 0:
        revenue_points = 0.0
    else:
        log_revenue = math.log10(max(revenue_estimate, 1))
        
        if log_revenue <= 6:  # Up to $1M
            revenue_points = (log_revenue / 6) * 7.5
        elif log_revenue <= 9:  # $1M to $1B
            revenue_points = 7.5 + ((log_revenue - 6) / 3) * 11.25
        elif log_revenue <= 11:  # $1B to $100B
            revenue_points = 18.75 + ((log_revenue - 9) / 2) * 7.5
        else:  # $100B+ (mega-corps)
            revenue_points = 26.25 + min(3.75, (log_revenue - 11) * 1.875)
    
    revenue_points = min(30, max(0, revenue_points))
    
    # =========================================================================
    # BRAND RECOGNITION (0-30 points) - Increased from 25
    # =========================================================================
    brand_score = max(1, min(10, brand_score))
    brand_points = (brand_score / 10) * 30
    
    # =========================================================================
    # MARKET LEADERSHIP (0-20 points) - Same
    # =========================================================================
    market_leader_score = max(1, min(10, market_leader_score))
    market_leader_points = (market_leader_score / 10) * 20
    
    # =========================================================================
    # GLOBAL PRESENCE (0-10 points) - Reduced from 15
    # =========================================================================
    global_presence_score = max(1, min(10, global_presence_score))
    global_presence_points = (global_presence_score / 10) * 10
    
    # =========================================================================
    # TECH/INNOVATION LEADERSHIP (0-10 points) - NEW
    # =========================================================================
    tech_innovation_score = max(1, min(10, tech_innovation_score))
    tech_innovation_points = (tech_innovation_score / 10) * 10
    
    # =========================================================================
    # TOTAL SCORE
    # =========================================================================
    raw_score = (
        revenue_points +
        brand_points +
        market_leader_points +
        global_presence_points +
        tech_innovation_points
    )
    
    # Normalize to 1-10 scale
    if raw_score == 0:
        return 0.0
    
    normalized_score = 1 + (raw_score / 100) * 9
    return round(normalized_score, 2)


def build_popularity_prompt(company: Dict) -> str:
    """
    Build prompt for LLM to estimate popularity metrics.
    
    CRITICAL: LLM estimates ALL metrics including revenue - CSV data is ignored.
    """
    return f"""You are an expert business analyst with comprehensive knowledge of global companies.
    Your task is to provide accurate popularity metrics for the company below.

    Company Information:
    - Name: {company.get("name", "")}
    - Domain: {company.get("domain", "")}
    - LinkedIn URL: {company.get("linkedin_url", "")}
    - Industry: {company.get("primary_industry", "")}
    - Country: {company.get("country", "")}
    - Location: {company.get("location", "")}
    - Description: {company.get("description", "")}
    - Employee Size: {company.get("size", "")}

    ⚠️ IMPORTANT: IGNORE any revenue data that may have been provided. 
    Estimate ALL metrics based on YOUR KNOWLEDGE of this company.

    Provide these 5 metrics:

    1. **estimated_revenue_usd**: YOUR estimate of annual revenue in USD.
    Reference scale:
    - Small business: $1M - $50M
    - Mid-market: $50M - $500M
    - Large enterprise: $500M - $5B
    - Major corporation: $5B - $50B
    - Fortune 100: $50B - $200B
    - Tech giants (Amazon, Apple, Microsoft, Google): $200B - $600B
    - Retail giant (Walmart): ~$650B
    
    ACCURATE EXAMPLES:
    - Amazon: ~$575,000,000,000
    - Apple: ~$385,000,000,000
    - Google/Alphabet: ~$307,000,000,000
    - Microsoft: ~$245,000,000,000
    - Walmart: ~$650,000,000,000
    - Tesla: ~$97,000,000,000

    2. **brand_recognition_score** (1-10): How famous is this brand globally?
    - 1-2: Unknown, local only
    - 3-4: Known regionally or in niche
    - 5-6: Known nationally or in industry
    - 7-8: Known internationally
    - 9: Famous worldwide (Nike, Coca-Cola, McDonald's)
    - 10: Ubiquitous - EVERYONE knows them (Amazon, Apple, Google, Microsoft)

    3. **market_leader_score** (1-10): Industry leadership and dominance
    - 1-2: Small player
    - 3-4: Established but not leading
    - 5-6: Top 10 in market
    - 7-8: Top 5 in market
    - 9: Top 2-3 globally
    - 10: Dominant #1 leader (Amazon in e-commerce, Google in search, Apple in premium devices)

    4. **global_presence_score** (1-10): Geographic reach
    - 1-2: Local/single city
    - 3-4: Single country
    - 5-6: Multi-country, one continent
    - 7-8: Multi-continent
    - 9: Most major markets
    - 10: Truly global - operates everywhere

    5. **tech_innovation_score** (1-10): Technology and innovation leadership
    - 1-2: Traditional, no tech focus
    - 3-4: Uses technology but not a leader
    - 5-6: Some innovation, digital presence
    - 7-8: Strong tech capabilities, some innovation
    - 9: Major tech innovator
    - 10: World's top tech leaders (Amazon, Apple, Google, Microsoft, Tesla)

    6. **is_commercial_entity**: true if for-profit, false if non-profit/NGO/government

    CRITICAL SCORING RULES:
    - Amazon, Apple, Google, Microsoft should get 10/10 on brand, market leadership, and tech
    - Walmart gets 10 brand, 10 market leadership, but only 5 tech (retail, not tech company)
    - Tesla gets 9-10 brand, 8-9 market, 10 tech (EV/innovation leader)
    - Traditional retailers like Aldi, 7-Eleven get 3-5 tech scores
    - Most unknown companies should be 3-5 range across all scores

    Return ONLY this JSON format:
    {{
        "estimated_revenue_usd": 575000000000,
        "brand_recognition_score": 10,
        "market_leader_score": 10,
        "global_presence_score": 10,
        "tech_innovation_score": 10,
        "is_commercial_entity": true
    }}"""


def infer_popularity_from_llm(company: Dict, max_retries: int = 3) -> Optional[Tuple[float, Dict]]:
    """
    Call LLM to get popularity metrics.
    
    Returns:
        Tuple of (popularity_index, raw_metrics) or None on error
    """
    retries = 0
    base_delay = 0.5
    
    while retries < max_retries:
        try:
            response = client.chat.completions.create(
                model=AZURE_DEPLOYMENT,
                messages=[{"role": "user", "content": build_popularity_prompt(company)}],
                temperature=0,
                max_tokens=300,
                response_format={"type": "json_object"}
            )
            
            content = response.choices[0].message.content.strip()
            metrics = json.loads(content)
            
            # Validate response
            required_keys = [
                "estimated_revenue_usd",
                "brand_recognition_score",
                "market_leader_score",
                "global_presence_score",
                "tech_innovation_score",
                "is_commercial_entity"
            ]
            if not all(key in metrics for key in required_keys):
                raise ValueError(f"Missing required keys in response: {metrics}")
            
            # Calculate popularity index
            popularity_index = calculate_popularity_index(
                float(metrics["estimated_revenue_usd"]),
                int(metrics["brand_recognition_score"]),
                int(metrics["market_leader_score"]),
                int(metrics["global_presence_score"]),
                int(metrics["tech_innovation_score"]),
                bool(metrics["is_commercial_entity"])
            )
            
            return (popularity_index, metrics)
            
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


def test_popularity_calculation():
    """Test the adjusted calculation with sample companies"""
    print("\n" + "="*80)
    print("🧪 TESTING ADJUSTED POPULARITY INDEX")
    print("="*80)
    print("\nNew Weighting:")
    print("  • Revenue: 30% (reduced from 40%)")
    print("  • Brand Recognition: 30% (increased from 25%)")
    print("  • Market Leadership: 20% (same)")
    print("  • Global Presence: 10% (reduced from 15%)")
    print("  • Tech/Innovation: 10% (NEW)")
    print()
    
    test_cases = [
        # Tech Giants - high tech scores
        {"name": "Amazon", "revenue": 575e9, "brand": 10, "market": 10, "global": 10, "tech": 10},
        {"name": "Apple", "revenue": 385e9, "brand": 10, "market": 10, "global": 10, "tech": 10},
        {"name": "Google (Alphabet)", "revenue": 307e9, "brand": 10, "market": 10, "global": 10, "tech": 10},
        {"name": "Microsoft", "revenue": 245e9, "brand": 10, "market": 10, "global": 10, "tech": 10},
        
        # Non-tech giants - lower tech scores
        {"name": "Walmart Inc.", "revenue": 650e9, "brand": 10, "market": 10, "global": 10, "tech": 5},
        {"name": "Tesla, Inc.", "revenue": 97e9, "brand": 9, "market": 8, "global": 9, "tech": 10},
        {"name": "McDonald's", "revenue": 23e9, "brand": 10, "market": 10, "global": 10, "tech": 4},
        {"name": "Nike, Inc.", "revenue": 51e9, "brand": 10, "market": 9, "global": 10, "tech": 5},
        {"name": "Nestlé S.A.", "revenue": 100e9, "brand": 9, "market": 9, "global": 10, "tech": 4},
        {"name": "LVMH", "revenue": 86e9, "brand": 8, "market": 10, "global": 9, "tech": 3},
        {"name": "Adidas AG", "revenue": 24e9, "brand": 9, "market": 8, "global": 9, "tech": 4},
        {"name": "Aldi", "revenue": 120e9, "brand": 7, "market": 8, "global": 7, "tech": 3},
    ]
    
    results = []
    for tc in test_cases:
        score = calculate_popularity_index(
            tc["revenue"], tc["brand"], tc["market"],
            tc["global"], tc["tech"], True
        )
        results.append({"name": tc["name"], "revenue": tc["revenue"], "score": score})
    
    results.sort(key=lambda x: x["score"], reverse=True)
    
    print(f"{'Rank':<5} {'Company':<25} {'Revenue':<20} {'Score (1-10)':<12}")
    print("-" * 65)
    
    for i, r in enumerate(results, 1):
        rev_str = f"${r['revenue']:,.0f}"
        marker = "🔥" if r['name'] in ['Amazon', 'Apple', 'Google (Alphabet)', 'Microsoft'] else "  "
        print(f"{i:<5} {marker} {r['name']:<22} {rev_str:<20} {r['score']:<12}")
    
    print("\n" + "="*80)
    print("✅ Amazon, Apple, Google, Microsoft now rank TOP 4!")
    print("="*80 + "\n")


def main():
    print("🚀 Starting Popularity Index Update (Tech Giants Priority)")
    print(f"⏰ Start time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📄 CSV file: {CSV_FILE_PATH}")
    print(f"👤 User ID: {USER_ID}")
    print("\n⚠️  NOTE: All metrics estimated by LLM (CSV revenue ignored)")
    print("📊 Weighting: Revenue 30% | Brand 30% | Market 20% | Global 10% | Tech 10%")
    
    # Run test first
    # test_popularity_calculation()
    
    # Uncomment below to run actual processing
    # """
    companies = read_csv_data(CSV_FILE_PATH)
    
    if not companies:
        print("❌ No companies found in CSV")
        return
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    total_count = len(companies)
    total_updated = 0
    total_skipped_not_in_db = 0
    total_skipped_llm_error = 0
    updates = []
    
    print(f"\\n{'='*60}")
    print(f"📊 Total companies in CSV: {total_count}")
    print(f"{'='*60}\\n")

    try:
        for idx, company in enumerate(companies):
            progress = idx + 1
            pct = (progress / total_count) * 100
            
            company_id = get_company_id(cur, USER_ID, company['name'])
            
            if company_id is None:
                total_skipped_not_in_db += 1
                print(f"[{progress}/{total_count}] ({pct:.1f}%) ⏭️ SKIP (not in DB): {company['name'][:50]}")
                continue
            
            result = infer_popularity_from_llm(company)
            
            if result is not None:
                popularity_index, metrics = result
                updates.append((popularity_index, company_id))
                total_updated += 1
                
                llm_rev = metrics.get('estimated_revenue_usd', 0)
                tech_score = metrics.get('tech_innovation_score', 0)
                print(f"[{progress}/{total_count}] ({pct:.1f}%) ✅ {company['name'][:30]:<30} "
                      f"Score: {popularity_index:.2f} | Rev: ${llm_rev/1e9:.1f}B | Tech: {tech_score}")
            else:
                total_skipped_llm_error += 1
                print(f"[{progress}/{total_count}] ({pct:.1f}%) ⚠️ LLM ERROR: {company['name'][:50]}")
            
            if len(updates) >= CHUNK_SIZE:
                print(f"\\n📦 Bulk updating {len(updates)} records...")
                bulk_update_popularity(cur, conn, updates)
                print(f"✅ Committed {len(updates)} updates\\n")
                updates = []
                time.sleep(0.3)
        
        if updates:
            print(f"\\n📦 Bulk updating remaining {len(updates)} records...")
            bulk_update_popularity(cur, conn, updates)
            print(f"✅ Committed {len(updates)} updates")
        
        print(f"\\n{'='*60}")
        print(f"✅ Processing Complete!")
        print(f"   - Total in CSV: {total_count}")
        print(f"   - Updated: {total_updated}")
        print(f"   - Skipped (not in DB): {total_skipped_not_in_db}")
        print(f"   - Skipped (LLM error): {total_skipped_llm_error}")
        print(f"{'='*60}")
        
    except KeyboardInterrupt:
        print("\\n⚠️ Process interrupted by user")
        if updates:
            print(f"💾 Saving {len(updates)} pending updates before exit...")
            bulk_update_popularity(cur, conn, updates)
    except Exception as e:
        print(f"\\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cur.close()
        conn.close()
        print(f"\\n⏰ End time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("✅ Database connection closed")
    # """


if __name__ == "__main__":
    main()
