"""
Company Master Upsert Script with LLM Enrichment

This script handles:
1. Reading company data from CSV
2. Checking if company exists in DB (by user_id + name)
3. If exists: UPDATE description, primary_industry, size, company_type, location, country, domain, linkedin_url
4. If not exists: INSERT new company with all data
5. LLM calls for:
   - revenue_range (using specified format)
   - employee_count (integer)
   - popularity_index (using existing calculation logic)

Company type conversion:
- "Public Company" -> "Public"
- "Private Company" or "Privately Held" -> "Private"
- Others: "Partnership", "Subsidiary", "Government", "Non-profit" (pass as-is)
"""

import psycopg2
import os
import time
import math
import json
import csv
import re
from openai import AzureOpenAI, RateLimitError
from psycopg2.extras import execute_batch
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Azure OpenAI config - USE ENVIRONMENT VARIABLES IN PRODUCTION
AZURE_OPENAI_KEY = os.getenv("AZURE_OPENAI_API_KEY", "YOUR_API_KEY_HERE")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT", "https://cs-ccr-poc-cinde-oai-1.openai.azure.com/")
AZURE_OPENAI_API_VERSION = "2025-01-01-preview"
AZURE_DEPLOYMENT = "gpt-4o"

client = AzureOpenAI(
    api_key=AZURE_OPENAI_KEY,
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_version=AZURE_OPENAI_API_VERSION
)

# Database config - USE ENVIRONMENT VARIABLES IN PRODUCTION
DB_CONFIG = {
    "database": os.getenv("DB_NAME", "postgres"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "your-super-secret-and-long-postgres-password"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", 5433),
}

# CSV file path
CSV_FILE_PATH = os.getenv("CSV_FILE_PATH", "companies.csv")

# User ID used during insertion/update
USER_ID = os.getenv("USER_ID", "632d6b76-0d5a-4074-b7e8-552b2a2aeb3d")

# Processing settings
CHUNK_SIZE = 100
LLM_RETRY_COUNT = 3
LLM_BASE_DELAY = 0.5


# =============================================================================
# COMPANY TYPE CONVERSION
# =============================================================================

def normalize_company_type(csv_type: str) -> Optional[str]:
    """
    Convert CSV company type to DB format.
    
    CSV values -> DB values:
    - "Public Company" -> "Public"
    - "Private Company" or "Privately Held" -> "Private"
    - "Partnership" -> "Partnership"
    - "Subsidiary" -> "Subsidiary"
    - "Government" -> "Government"
    - "Non-profit" or "Nonprofit" -> "Non-profit"
    """
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

def read_csv_data(file_path: str) -> List[Dict]:
    """
    Read company data from CSV file.
    
    Expected CSV columns (from LinkedIn export or similar):
    - Find companies (empty), Name, Description, Primary Industry, Size, Type, Location, Country, Domain, LinkedIn URL
    
    Also handles standard format:
    - Name, Description, Primary Industry, Size, Type, Location, Country, Domain, LinkedIn URL
    """
    companies = []
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            # Print detected columns for debugging
            print(f"📋 Detected CSV columns: {reader.fieldnames}")
            
            for row in reader:
                # Handle potential column name variations (case-insensitive lookup)
                def get_field(primary_key, *alternates):
                    """Get field value with fallbacks for different column names"""
                    for key in [primary_key] + list(alternates):
                        if key in row and row[key]:
                            return row[key].strip()
                        # Try lowercase
                        key_lower = key.lower()
                        for csv_key in row.keys():
                            if csv_key.lower() == key_lower and row[csv_key]:
                                return row[csv_key].strip()
                    return ''
                
                name = get_field('Name', 'name', 'Company Name', 'company_name')
                
                if not name:
                    continue
                
                # Clean description (remove extra whitespace and newlines)
                description = get_field('Description', 'description')
                description = ' '.join(description.split()) if description else ''
                    
                company = {
                    'name': name,
                    'description': description,
                    'primary_industry': get_field('Primary Industry', 'primary_industry', 'Industry'),
                    'size': get_field('Size', 'size', 'Company Size', 'Employee Count'),
                    'company_type_raw': get_field('Type', 'type', 'Company Type'),
                    'location': get_field('Location', 'location', 'Headquarters'),
                    'country': get_field('Country', 'country'),
                    'domain': get_field('Domain', 'domain', 'Website'),
                    'linkedin_url': get_field('LinkedIn URL', 'linkedin_url', 'LinkedIn'),
                }
                
                # Normalize company type
                company['company_type'] = normalize_company_type(company['company_type_raw'])
                
                companies.append(company)
                
        print(f"📄 Read {len(companies)} companies from CSV")
    except FileNotFoundError:
        print(f"❌ CSV file not found: {file_path}")
        raise
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        raise
    return companies


# =============================================================================
# DATABASE OPERATIONS
# =============================================================================

def get_company_by_name(cur, user_id: str, name: str) -> Optional[Dict]:
    """Check if company exists in DB and return its data, otherwise return None"""
    query = """
        SELECT id, name, description, primary_industry, size, company_type, 
               location, country, domain, linkedin_url, revenue_range, 
               employee_count, popularity_index
        FROM company_master 
        WHERE name = %s
    """
    cur.execute(query, (name,))
    result = cur.fetchone()
    if result:
        return {
            'id': result[0],
            'name': result[1],
            'description': result[2],
            'primary_industry': result[3],
            'size': result[4],
            'company_type': result[5],
            'location': result[6],
            'country': result[7],
            'domain': result[8],
            'linkedin_url': result[9],
            'revenue_range': result[10],
            'employee_count': result[11],
            'popularity_index': result[12],
        }
    return None


def update_company_basic_info(cur, company_id: str, company_data: Dict):
    """Update basic company info from CSV"""
    query = """
        UPDATE company_master 
        SET description = %s,
            primary_industry = %s,
            size = %s,
            company_type = %s,
            location = %s,
            country = %s,
            domain = %s,
            linkedin_url = %s,
            updated_at = NOW()
        WHERE id = %s
    """
    cur.execute(query, (
        company_data.get('description') or None,
        company_data.get('primary_industry') or None,
        company_data.get('size') or None,
        company_data.get('company_type') or None,
        company_data.get('location') or None,
        company_data.get('country') or None,
        company_data.get('domain') or None,
        company_data.get('linkedin_url') or None,
        company_id
    ))


def insert_new_company(cur, user_id: str, company_data: Dict) -> str:
    """Insert a new company and return its ID"""
    query = """
        INSERT INTO company_master (
            user_id, name, description, primary_industry, size, company_type,
            location, country, domain, linkedin_url, processing_status, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending', NOW(), NOW()
        )
        RETURNING id
    """
    cur.execute(query, (
        user_id,
        company_data['name'],
        company_data.get('description') or None,
        company_data.get('primary_industry') or None,
        company_data.get('size') or None,
        company_data.get('company_type') or None,
        company_data.get('location') or None,
        company_data.get('country') or None,
        company_data.get('domain') or None,
        company_data.get('linkedin_url') or None,
    ))
    result = cur.fetchone()
    return result[0]


def update_enrichment_data(cur, company_id: str, revenue_range: str, employee_count: int, popularity_index: float):
    """Update LLM-enriched fields"""
    query = """
        UPDATE company_master 
        SET revenue_range = %s,
            employee_count = %s,
            popularity_index = %s,
            last_enriched_at = NOW(),
            updated_at = NOW()
        WHERE id = %s
    """
    cur.execute(query, (revenue_range, employee_count, popularity_index, company_id))


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
    """
    Calculate popularity index with adjusted weighting to prioritize tech giants.
    
    WEIGHTING:
    - Revenue: 30 points
    - Brand Recognition: 30 points
    - Market Leadership: 20 points
    - Global Presence: 10 points
    - Tech/Innovation: 10 points
    
    Total: 100 points, normalized to 1-10 scale
    """
    if not is_commercial:
        return 0.0
    
    # REVENUE COMPONENT (0-30 points)
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
        else:  # $100B+
            revenue_points = 26.25 + min(3.75, (log_revenue - 11) * 1.875)
    
    revenue_points = min(30, max(0, revenue_points))
    
    # BRAND RECOGNITION (0-30 points)
    brand_score = max(1, min(10, brand_score))
    brand_points = (brand_score / 10) * 30
    
    # MARKET LEADERSHIP (0-20 points)
    market_leader_score = max(1, min(10, market_leader_score))
    market_leader_points = (market_leader_score / 10) * 20
    
    # GLOBAL PRESENCE (0-10 points)
    global_presence_score = max(1, min(10, global_presence_score))
    global_presence_points = (global_presence_score / 10) * 10
    
    # TECH/INNOVATION (0-10 points)
    tech_innovation_score = max(1, min(10, tech_innovation_score))
    tech_innovation_points = (tech_innovation_score / 10) * 10
    
    # TOTAL SCORE
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


# =============================================================================
# LLM PROMPTS AND CALLS
# =============================================================================

def build_enrichment_prompt(company: Dict) -> str:
    """
    Build prompt for LLM to infer revenue_range, employee_count, and popularity metrics.
    """
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


def infer_enrichment_from_llm(company: Dict, max_retries: int = LLM_RETRY_COUNT) -> Optional[Dict]:
    """
    Call LLM to get enrichment data.
    
    Returns:
        Dict with revenue_range, employee_count, and popularity_index, or None on error
    """
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
            
            # Validate required keys
            required_keys = [
                "revenue_range",
                "employee_count",
                "estimated_revenue_usd",
                "brand_recognition_score",
                "market_leader_score",
                "global_presence_score",
                "tech_innovation_score",
                "is_commercial_entity"
            ]
            if not all(key in metrics for key in required_keys):
                missing = [k for k in required_keys if k not in metrics]
                raise ValueError(f"Missing required keys in response: {missing}")
            
            # Validate revenue_range format
            valid_ranges = [
                "$500B+", "$200B+", "$100B+", "$50B+", "$25B+", "$10B+",
                "$5B+", "$1B+", "$500M+", "$100M+", "$50M+", "<$50M"
            ]
            if metrics["revenue_range"] not in valid_ranges:
                print(f"⚠️ Invalid revenue_range '{metrics['revenue_range']}', using closest match")
                # Try to fix common issues
                metrics["revenue_range"] = fix_revenue_range(metrics["revenue_range"])
            
            # Calculate popularity index
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
                'raw_metrics': metrics
            }
            
        except RateLimitError as e:
            retries += 1
            if retries >= max_retries:
                print(f"❌ Max retries for {company.get('name', '?')}")
                return None
            
            retry_after = getattr(e, 'retry_after', None)
            wait_time = retry_after if retry_after else LLM_BASE_DELAY * (2 ** (retries - 1))
            
            print(f"⏳ Rate limit. Waiting {wait_time:.1f}s (retry {retries}/{max_retries})")
            time.sleep(wait_time)
        except json.JSONDecodeError as e:
            print(f"❌ JSON parsing error for {company.get('name', '?')}: {str(e)[:100]}")
            return None
        except Exception as e:
            print(f"❌ Error for {company.get('name', '?')}: {str(e)[:100]}")
            return None
    
    return None


def fix_revenue_range(value: str) -> str:
    """Attempt to fix common revenue_range format issues"""
    value = value.strip().upper()
    
    # Common fixes
    fixes = {
        "$500B": "$500B+",
        "$200B": "$200B+",
        "$100B": "$100B+",
        "$50B": "$50B+",
        "$25B": "$25B+",
        "$10B": "$10B+",
        "$5B": "$5B+",
        "$1B": "$1B+",
        "$500M": "$500M+",
        "$100M": "$100M+",
        "$50M": "$50M+",
        "LESS THAN $50M": "<$50M",
        "UNDER $50M": "<$50M",
    }
    
    return fixes.get(value, "<$50M")


# =============================================================================
# MAIN PROCESSING
# =============================================================================

def process_companies():
    """Main function to process companies from CSV"""
    print("🚀 Starting Company Upsert with Enrichment")
    print(f"⏰ Start time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📄 CSV file: {CSV_FILE_PATH}")
    print(f"👤 User ID: {USER_ID}")
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
    total_enriched = 0
    total_llm_errors = 0
    
    print(f"\n📊 Total companies in CSV: {total_count}")
    print("=" * 70 + "\n")
    
    try:
        for idx, company in enumerate(companies):
            progress = idx + 1
            pct = (progress / total_count) * 100
            company_name = company['name'][:40]
            
            # Check if company exists
            existing = get_company_by_name(cur, USER_ID, company['name'])
            
            if existing:
                # UPDATE existing company
                company_id = existing['id']
                update_company_basic_info(cur, company_id, company)
                total_updated += 1
                action = "UPDATE"
            else:
                # INSERT new company
                company_id = insert_new_company(cur, USER_ID, company)
                total_inserted += 1
                action = "INSERT"
            
            conn.commit()
            
            # LLM enrichment for revenue_range, employee_count, popularity_index
            enrichment = infer_enrichment_from_llm(company)
            
            if enrichment:
                update_enrichment_data(
                    cur, 
                    company_id, 
                    enrichment['revenue_range'],
                    enrichment['employee_count'],
                    enrichment['popularity_index']
                )
                conn.commit()
                total_enriched += 1
                
                print(f"[{progress}/{total_count}] ({pct:.1f}%) ✅ {action}: {company_name:<40} "
                      f"| Rev: {enrichment['revenue_range']:<8} | Emp: {enrichment['employee_count']:>8,} "
                      f"| Pop: {enrichment['popularity_index']:.2f}")
            else:
                total_llm_errors += 1
                print(f"[{progress}/{total_count}] ({pct:.1f}%) ⚠️ {action}: {company_name:<40} "
                      f"| LLM enrichment failed")
            
            # Small delay to avoid rate limits
            time.sleep(0.2)
        
        # Final summary
        print("\n" + "=" * 70)
        print("✅ Processing Complete!")
        print(f"   - Total in CSV: {total_count}")
        print(f"   - Inserted: {total_inserted}")
        print(f"   - Updated: {total_updated}")
        print(f"   - Enriched (LLM success): {total_enriched}")
        print(f"   - LLM errors: {total_llm_errors}")
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


def test_company_type_conversion():
    """Test the company type conversion"""
    print("\n🧪 Testing Company Type Conversion")
    print("-" * 40)
    
    test_cases = [
        ("Public Company", "Public"),
        ("Private Company", "Private"),
        ("Privately Held", "Private"),
        ("Partnership", "Partnership"),
        ("Subsidiary", "Subsidiary"),
        ("Government", "Government"),
        ("Non-profit", "Non-profit"),
        ("Nonprofit", "Non-profit"),
        ("", None),
        ("  Public Company  ", "Public"),
    ]
    
    all_passed = True
    for input_val, expected in test_cases:
        result = normalize_company_type(input_val)
        status = "✅" if result == expected else "❌"
        if result != expected:
            all_passed = False
        print(f"{status} '{input_val}' -> '{result}' (expected: '{expected}')")
    
    print("-" * 40)
    if all_passed:
        print("✅ All tests passed!")
    else:
        print("❌ Some tests failed!")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        test_company_type_conversion()
    else:
        process_companies()