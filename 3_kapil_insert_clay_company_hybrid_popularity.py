"""
Company Master Batch Upsert Script with HYBRID Popularity Index

POPULARITY INDEX LOGIC:
========================

1. INITIAL CSV LOAD:
   - popularity_index = MIN + ((N - row_number) / (N - 1)) * (MAX - MIN)
   - Range: [MIN, MAX] where first company = MAX, last = MIN
   - Preserves exact CSV order

2. NEW COMPANIES (not in original CSV):
   - Calculated from: brand_recognition, revenue, size, market_leader, global_presence
   - Formula produces score in [MIN, MAX] range
   - New companies slot into correct position based on their attributes

3. SUBSIDIARIES:
   - Identified by matching parent company name patterns
   - Score = parent_score - 0.0001 (appears just below parent)
   - If parent not found, calculated normally

CONFIGURABLE PARAMETERS:
- POPULARITY_INDEX_MIN: Lower bound (default: 10)
- POPULARITY_INDEX_MAX: Upper bound (default: 15)

DEDUPLICATION:
- Keeps FIRST occurrence only

LLM ENRICHMENT:
- Still enriches revenue_range and employee_count
- Also gets brand/market scores for NEW companies only
"""

import psycopg2
import os
import time
import math
import json
import csv
import re
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
# CSV_FILE_PATH = os.getenv("CSV_FILE_PATH", "clay_company/retail.csv")
# CSV_ALL_FILES = ['Advertising_Services.csv', 'Banking.csv', 'Computer_and_Network_Security.csv', 'Data_Infrastructure_and_Analytics.csv', 'Financial_Services.csv', 'Insurance.csv', 'IT_Services_and_IT_Consulting.csv', 'Marketing_Services.csv', 'Venture_Capital_and_Private_Equity.csv']
# CSV_ALL_FILES = ['Marketing_Services.csv', 'Venture_Capital_and_Private_Equity.csv']
# CSV_ALL_FILES = ['Accounting.csv', 'Biotechnology.csv', 'Biotechnology_Research.csv', 'Brokerages.csv', 'Business_Consulting_and_Services.csv', 'Business_Intelligence_Platforms.csv', 'Capital_Markets.csv', 'Commercial_Real_Estate.csv', 'Data_Security Software Products.csv', 'Data_Security_Software_Products.csv', 'E-Learning_Providers.csv', 'Education_Management.csv', 'Freight_and_Package_Transportation.csv', 'Higher_Education.csv', 'Hospitals.csv', 'Hospitals_and_Health_Care.csv', 'Human_Resources_Services.csv', 'Import_and_Export.csv', 'Information_Technology.csv', 'Internet_Marketplace_Platforms.csv', 'Investment_Banking.csv', 'Investment_Management.csv', 'Market_Research.csv', 'Media_&_Telecommunications.csv', 'Medical_Devices.csv', 'Operations_Consulting.csv', 'OutsourcingOffshoring.csv', 'Outsourcing_and_Offshoring.csv', 'Pharmaceutical_Manufacturing.csv', 'Public_Relations.csv', 'Real_Estate.csv', 'Staffing_and_Recruiting.csv', 'Strategic_Management_Services.csv', 'Technology_Information_and_Media.csv', 'Telecommunications.csv', 'Telecommunications_Carriers.csv', 'Transportation_Logistics_Supply.csv', 'Warehousing_and_Storage.csv']
# CSV_ALL_FILES = ['Import_and_Export.csv', 'Information_Technology.csv', 'Internet_Marketplace_Platforms.csv', 'Investment_Banking.csv', 'Investment_Management.csv', 'Market_Research.csv', 'Media_&_Telecommunications.csv', 'Medical_Devices.csv', 'Operations_Consulting.csv', 'OutsourcingOffshoring.csv', 'Outsourcing_and_Offshoring.csv', 'Pharmaceutical_Manufacturing.csv', 'Public_Relations.csv', 'Real_Estate.csv', 'Staffing_and_Recruiting.csv', 'Strategic_Management_Services.csv', 'Technology_Information_and_Media.csv', 'Telecommunications.csv', 'Telecommunications_Carriers.csv', 'Transportation_Logistics_Supply.csv', 'Warehousing_and_Storage.csv']
# CSV_ALL_FILES = ['Warehousing_and_Storage.csv']
CSV_ALL_FILES = ['Automation Machinery.csv', 'Building Construction.csv', 'Building Materials.csv', 'Construction.csv', 'Equipment Rental Services.csv', 'Industrial Automation.csv', 'Industrial Machinery Manufacturing.csv', 'Machinery Manufacturing.csv', 'Manufacturing.csv', 'Online and Mail Order Retail.csv', 'Specialty Trade Contractors.csv', 'Wholesale Building Materials.csv', 'Wholesale Chemical and Allied.csv', 'Wholesale Computer Equipment.csv', 'Wholesale Drugs and Sundries.csv', 'Wholesale Import and Export.csv', 'Wholesale Machinery.csv', 'Wholesale.csv']
# User ID
USER_ID = os.getenv("USER_ID", "632d6b76-0d5a-4074-b7e8-552b2a2aeb3d")

# Processing settings
BATCH_SIZE = 50
LLM_RETRY_COUNT = 3
LLM_BASE_DELAY = 0.5
USE_PARALLEL_LLM = True
MAX_LLM_WORKERS = 5

# Mode: 'initial' for first CSV load, 'incremental' for new companies
LOAD_MODE = os.getenv("LOAD_MODE", "initial")  # 'initial' or 'incremental'

# Popularity Index Range Configuration (CONFIGURABLE)
POPULARITY_INDEX_MIN = float(os.getenv("POPULARITY_INDEX_MIN", "10"))
POPULARITY_INDEX_MAX = float(os.getenv("POPULARITY_INDEX_MAX", "15"))


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
# SIZE TO NUMERIC CONVERSION (for scoring)
# =============================================================================

def size_to_employee_estimate(size_str: str) -> int:
    """Convert size string to estimated employee count."""
    if not size_str:
        return 50
    
    size_lower = size_str.lower().strip()
    
    size_mapping = {
        "self-employed": 1,
        "1": 1,
        "2-10 employees": 5,
        "2-10": 5,
        "11-50 employees": 30,
        "11-50": 30,
        "51-200 employees": 125,
        "51-200": 125,
        "201-500 employees": 350,
        "201-500": 350,
        "501-1,000 employees": 750,
        "501-1000 employees": 750,
        "501-1,000": 750,
        "1,001-5,000 employees": 3000,
        "1001-5000 employees": 3000,
        "1,001-5,000": 3000,
        "5,001-10,000 employees": 7500,
        "5001-10000 employees": 7500,
        "5,001-10,000": 7500,
        "10,001+ employees": 50000,
        "10001+ employees": 50000,
        "10,001+": 50000,
    }
    
    for key, value in size_mapping.items():
        if key in size_lower:
            return value
    
    return 50  # Default


def size_to_score(size_str: str) -> int:
    """Convert company size to a 1-10 score."""
    employees = size_to_employee_estimate(size_str)
    
    if employees >= 50000:
        return 10
    elif employees >= 10000:
        return 9
    elif employees >= 5000:
        return 8
    elif employees >= 1000:
        return 7
    elif employees >= 500:
        return 6
    elif employees >= 200:
        return 5
    elif employees >= 50:
        return 4
    elif employees >= 10:
        return 3
    elif employees >= 2:
        return 2
    else:
        return 1


# =============================================================================
# SUBSIDIARY DETECTION
# =============================================================================

def detect_parent_company(company_name: str, all_company_names: set) -> Optional[str]:
    """
    Detect if a company is a subsidiary and find its parent.
    
    Patterns detected:
    - "Parent Company - Division"
    - "Parent Company (Region)"
    - "Parent Company China/India/etc"
    - "Parent Company LATAM/EMEA/APAC"
    - "Parent Labs" -> "Parent"
    """
    name = company_name.strip()
    
    # Pattern 1: "Company - Division" or "Company | Division"
    for separator in [' - ', ' | ', ' – ']:
        if separator in name:
            potential_parent = name.split(separator)[0].strip()
            if potential_parent.lower() in all_company_names:
                return potential_parent
    
    # Pattern 2: "Company (Region)" or "Company (Division)"
    paren_match = re.match(r'^(.+?)\s*\([^)]+\)$', name)
    if paren_match:
        potential_parent = paren_match.group(1).strip()
        if potential_parent.lower() in all_company_names:
            return potential_parent
    
    # Pattern 3: "Company Region" where region is country/area
    regions = ['china', 'india', 'japan', 'korea', 'brazil', 'latam', 'emea', 
               'apac', 'americas', 'europe', 'asia', 'uk', 'us', 'usa',
               '中国', '日本', 'deutschland', 'france', 'españa']
    
    name_lower = name.lower()
    for region in regions:
        if name_lower.endswith(' ' + region):
            potential_parent = name[:-len(region)-1].strip()
            if potential_parent.lower() in all_company_names:
                return potential_parent
    
    # Pattern 4: Common subsidiary suffixes
    suffixes = [' Labs', ' Lab', ' Studio', ' Studios', ' Games', ' Cloud', 
                ' AI', ' Tech', ' Digital', ' Solutions', ' Services',
                ' Research', ' Ventures', ' Capital', ' Foundation']
    for suffix in suffixes:
        if name.endswith(suffix):
            potential_parent = name[:-len(suffix)].strip()
            if potential_parent.lower() in all_company_names:
                return potential_parent
    
    return None


# =============================================================================
# POPULARITY INDEX CALCULATIONS
# =============================================================================

def calculate_position_based_index(row_number: int, total_rows: int, 
                                    min_val: float = None, max_val: float = None) -> float:
    """
    Calculate popularity index from CSV position (for initial load).
    
    Formula: MIN + ((N - row_number) / (N - 1)) * (MAX - MIN)
    Range: [MIN, MAX] where first=MAX, last=MIN
    
    Args:
        row_number: 1-based row number (1 = first company)
        total_rows: Total number of unique companies (N)
        min_val: Minimum popularity index value (default: POPULARITY_INDEX_MIN)
        max_val: Maximum popularity index value (default: POPULARITY_INDEX_MAX)
    
    Returns:
        Popularity index in range [min_val, max_val]
    """
    min_val = min_val if min_val is not None else POPULARITY_INDEX_MIN
    max_val = max_val if max_val is not None else POPULARITY_INDEX_MAX
    
    if total_rows <= 1:
        return max_val
    
    index_range = max_val - min_val
    popularity = min_val + ((total_rows - row_number) / (total_rows - 1)) * index_range
    return round(popularity, 6)


def calculate_attribute_based_index(
    brand_recognition_score: int,
    revenue_estimate: float,
    size_score: int,
    market_leader_score: int,
    global_presence_score: int,
    tech_innovation_score: int,
    is_commercial: bool,
    min_val: float = None,
    max_val: float = None
) -> float:
    """
    Calculate popularity index from company attributes (for new companies).
    
    This formula is designed to produce scores that align with the CSV ordering:
    - Top companies (Google, Amazon, Microsoft) get close to MAX
    - Mid-tier companies get middle of range
    - Small/unknown companies get close to MIN
    
    Weights:
    - Brand Recognition: 30% (most important for ordering)
    - Revenue: 25%
    - Market Leadership: 20%
    - Company Size: 10%
    - Global Presence: 10%
    - Tech Innovation: 5%
    
    Args:
        min_val: Minimum popularity index value (default: POPULARITY_INDEX_MIN)
        max_val: Maximum popularity index value (default: POPULARITY_INDEX_MAX)
    
    Returns: Score in range [min_val, max_val]
    """
    min_val = min_val if min_val is not None else POPULARITY_INDEX_MIN
    max_val = max_val if max_val is not None else POPULARITY_INDEX_MAX
    index_range = max_val - min_val
    
    if not is_commercial:
        # Non-profits get low but not minimum score
        return round(min_val + (index_range * 0.05), 6)
    
    # Normalize all scores to 0-1 range
    brand_norm = max(1, min(10, brand_recognition_score)) / 10
    market_norm = max(1, min(10, market_leader_score)) / 10
    size_norm = max(1, min(10, size_score)) / 10
    global_norm = max(1, min(10, global_presence_score)) / 10
    tech_norm = max(1, min(10, tech_innovation_score)) / 10
    
    # Revenue score (logarithmic scale)
    if revenue_estimate <= 0:
        revenue_norm = 0.1
    else:
        # $1M = 0.1, $1B = 0.5, $100B = 0.9, $500B+ = 1.0
        log_revenue = math.log10(max(revenue_estimate, 1_000_000))
        revenue_norm = min(1.0, max(0.1, (log_revenue - 6) / 6))
    
    # Weighted combination
    weighted_score = (
        brand_norm * 0.30 +
        revenue_norm * 0.25 +
        market_norm * 0.20 +
        size_norm * 0.10 +
        global_norm * 0.10 +
        tech_norm * 0.05
    )
    
    # Scale to configured range
    popularity = min_val + weighted_score * index_range
    return round(popularity, 6)


def calculate_subsidiary_index(parent_index: float, subsidiary_order: int = 1) -> float:
    """
    Calculate popularity index for a subsidiary.
    
    Subsidiaries appear just below their parent company.
    Each subsidiary gets a slightly lower score.
    
    Args:
        parent_index: The parent company's popularity_index
        subsidiary_order: 1 for first subsidiary, 2 for second, etc.
    
    Returns: Score slightly below parent (by 0.00001 per subsidiary)
             Using smaller decrement to fit within narrower range
    """
    return round(parent_index - (0.00001 * subsidiary_order), 6)


# =============================================================================
# CSV READING
# =============================================================================

def read_csv_data(file_path: str, deduplicate: bool = True) -> List[Dict]:
    """
    Read company data from CSV file.
    
    - Assigns row_number (1-based) to each company
    - Deduplication keeps FIRST occurrence
    """
    companies = []
    seen_names = set()
    duplicates_in_csv = 0
    row_number = 0
    
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            print(f"📋 Detected CSV columns: {reader.fieldnames}")
            
            for idx, row in enumerate(reader):
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
                
                row_number += 1
                
                # Deduplication - keep FIRST occurrence only
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
                    'row_number': row_number,
                }
                companies.append(company)
        
        print(f"📄 Read {len(companies)} unique companies from CSV")
        if duplicates_in_csv > 0:
            print(f"⚠️  Skipped {duplicates_in_csv} duplicate entries (kept first occurrence)")
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        raise
    
    return companies


# =============================================================================
# LLM ENRICHMENT
# =============================================================================

def build_enrichment_prompt_initial(company: Dict) -> str:
    """Build prompt for INITIAL load - only revenue and employee count."""
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

2. **employee_count**: Estimated total number of employees as an integer

Return ONLY this JSON format:
{{
    "revenue_range": "$1B+",
    "employee_count": 150000
}}"""


def build_enrichment_prompt_incremental(company: Dict) -> str:
    """Build prompt for INCREMENTAL load - includes scoring metrics for popularity calculation."""
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

Provide ALL of the following information:

1. **revenue_range**: Estimated annual revenue using ONLY these exact formats:
- "$500B+" / "$200B+" / "$100B+" / "$50B+" / "$25B+" / "$10B+" / "$5B+" / "$1B+" / "$500M+" / "$100M+" / "$50M+" / "<$50M"

2. **employee_count**: Estimated total employees as integer

3. **estimated_revenue_usd**: Annual revenue in USD (number only, e.g., 5000000000)

4. **brand_recognition_score** (1-10): How famous is this brand globally?
- 1-2: Unknown, local only
- 3-4: Known regionally or in niche
- 5-6: Known nationally or in industry
- 7-8: Known internationally
- 9-10: Famous worldwide (Google, Amazon, Apple level)

5. **market_leader_score** (1-10): Industry leadership
- 1-2: Small player
- 5-6: Top 10 in market
- 9-10: Dominant #1 leader

6. **global_presence_score** (1-10): Geographic reach
- 1-2: Local/single city
- 5-6: Multi-country
- 9-10: Truly global

7. **tech_innovation_score** (1-10): Technology leadership
- 1-2: Traditional, no tech focus
- 5-6: Some innovation
- 9-10: World's top tech leaders

8. **is_commercial_entity**: true if for-profit, false if non-profit/NGO/government

9. **parent_company**: If this is a subsidiary, provide the parent company name. Otherwise null.

Return ONLY this JSON format:
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


def infer_enrichment_from_llm(company: Dict, mode: str = 'initial', max_retries: int = LLM_RETRY_COUNT) -> Optional[Dict]:
    """Call LLM to get enrichment data."""
    retries = 0
    
    prompt_func = build_enrichment_prompt_initial if mode == 'initial' else build_enrichment_prompt_incremental
    
    while retries < max_retries:
        try:
            response = client.chat.completions.create(
                model=AZURE_DEPLOYMENT,
                messages=[{"role": "user", "content": prompt_func(company)}],
                temperature=0,
                max_tokens=500,
                response_format={"type": "json_object"}
            )
            
            content = response.choices[0].message.content.strip()
            metrics = json.loads(content)
            
            # Validate required keys
            required_keys = ["revenue_range", "employee_count"]
            if mode == 'incremental':
                required_keys.extend([
                    "estimated_revenue_usd", "brand_recognition_score",
                    "market_leader_score", "global_presence_score",
                    "tech_innovation_score", "is_commercial_entity"
                ])
            
            if not all(key in metrics for key in required_keys):
                raise ValueError(f"Missing keys: {[k for k in required_keys if k not in metrics]}")
            
            # Fix revenue_range
            valid_ranges = ["$500B+", "$200B+", "$100B+", "$50B+", "$25B+", "$10B+",
                          "$5B+", "$1B+", "$500M+", "$100M+", "$50M+", "<$50M"]
            if metrics["revenue_range"] not in valid_ranges:
                metrics["revenue_range"] = fix_revenue_range(metrics["revenue_range"])
            
            result = {
                'revenue_range': metrics["revenue_range"],
                'employee_count': int(metrics["employee_count"]),
            }
            
            if mode == 'incremental':
                result.update({
                    'estimated_revenue_usd': float(metrics["estimated_revenue_usd"]),
                    'brand_recognition_score': int(metrics["brand_recognition_score"]),
                    'market_leader_score': int(metrics["market_leader_score"]),
                    'global_presence_score': int(metrics["global_presence_score"]),
                    'tech_innovation_score': int(metrics["tech_innovation_score"]),
                    'is_commercial_entity': bool(metrics["is_commercial_entity"]),
                    'parent_company': metrics.get("parent_company"),
                })
            
            return result
            
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


def enrich_companies_parallel(companies: List[Dict], mode: str = 'initial') -> List[Tuple[Dict, Optional[Dict]]]:
    """Enrich multiple companies in parallel."""
    results = []
    
    if USE_PARALLEL_LLM and len(companies) > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_LLM_WORKERS) as executor:
            future_to_company = {
                executor.submit(infer_enrichment_from_llm, c, mode): c 
                for c in companies
            }
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
            enrichment = infer_enrichment_from_llm(company, mode)
            results.append((company, enrichment))
    
    return results


# =============================================================================
# DATABASE OPERATIONS
# =============================================================================

def get_existing_companies(cur, user_id: str) -> Dict[str, float]:
    """Get existing companies and their popularity indices from DB."""
    cur.execute("""
        SELECT LOWER(name), popularity_index 
        FROM company_master 
        WHERE user_id = %s AND popularity_index IS NOT NULL
    """, (user_id,))
    
    return {row[0]: row[1] for row in cur.fetchall()}


def batch_upsert_companies(
    cur, 
    user_id: str, 
    enriched_data: List[Tuple[Dict, Optional[Dict]]], 
    total_rows: int,
    mode: str = 'initial',
    existing_companies: Dict[str, float] = None
) -> Tuple[int, int, int, int]:
    """
    Batch upsert companies using ON CONFLICT.
    
    In 'initial' mode: popularity_index from row_number
    In 'incremental' mode: popularity_index from attributes or subsidiary logic
    """
    if not enriched_data:
        return 0, 0, 0, 0
    
    existing_companies = existing_companies or {}
    all_company_names = set(existing_companies.keys())
    
    # Add current batch names to the set
    for company, _ in enriched_data:
        all_company_names.add(company['name'].strip().lower())
    
    # Deduplicate - keep FIRST occurrence
    seen_names = {}
    duplicates_removed = 0
    
    for company, enrichment in enriched_data:
        name = company['name'].strip().lower()
        if name not in seen_names:
            seen_names[name] = (company, enrichment)
        else:
            duplicates_removed += 1
    
    deduplicated_data = list(seen_names.values())
    
    if duplicates_removed > 0:
        print(f"    ⚠️ Removed {duplicates_removed} duplicates (kept first)")
    
    # Prepare data for upsert
    upsert_values = []
    llm_errors = 0
    subsidiary_counts = {}  # Track subsidiaries per parent
    
    for company, enrichment in deduplicated_data:
        # Calculate popularity_index based on mode
        if mode == 'initial':
            # Position-based for initial load
            row_number = company.get('row_number', 1)
            popularity_index = calculate_position_based_index(row_number, total_rows)
        else:
            # Attribute-based for incremental load
            if enrichment:
                # Check for subsidiary
                parent_company = enrichment.get('parent_company')
                if not parent_company:
                    parent_company = detect_parent_company(company['name'], all_company_names)
                
                if parent_company:
                    parent_lower = parent_company.lower()
                    if parent_lower in existing_companies:
                        # Subsidiary: score just below parent
                        subsidiary_counts[parent_lower] = subsidiary_counts.get(parent_lower, 0) + 1
                        popularity_index = calculate_subsidiary_index(
                            existing_companies[parent_lower],
                            subsidiary_counts[parent_lower]
                        )
                    else:
                        # Parent not found, calculate normally
                        popularity_index = calculate_attribute_based_index(
                            enrichment['brand_recognition_score'],
                            enrichment['estimated_revenue_usd'],
                            size_to_score(company.get('size', '')),
                            enrichment['market_leader_score'],
                            enrichment['global_presence_score'],
                            enrichment['tech_innovation_score'],
                            enrichment['is_commercial_entity']
                        )
                else:
                    # Not a subsidiary, calculate from attributes
                    popularity_index = calculate_attribute_based_index(
                        enrichment['brand_recognition_score'],
                        enrichment['estimated_revenue_usd'],
                        size_to_score(company.get('size', '')),
                        enrichment['market_leader_score'],
                        enrichment['global_presence_score'],
                        enrichment['tech_innovation_score'],
                        enrichment['is_commercial_entity']
                    )
            else:
                # No enrichment data, use size as fallback
                size_score = size_to_score(company.get('size', ''))
                # Scale size_score (1-10) to configured range
                index_range = POPULARITY_INDEX_MAX - POPULARITY_INDEX_MIN
                popularity_index = POPULARITY_INDEX_MIN + (size_score / 10) * index_range
        
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
                popularity_index,
            ))
        else:
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
                None,
                None,
                popularity_index,
            ))
    
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
            popularity_index = EXCLUDED.popularity_index,
            updated_at = NOW(),
            last_enriched_at = CASE WHEN EXCLUDED.revenue_range IS NOT NULL THEN NOW() ELSE company_master.last_enriched_at END
        RETURNING (xmax = 0) AS inserted
    """
    
    template = """(
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        'completed', NOW(), NOW(), 
        CASE WHEN %s IS NOT NULL THEN NOW() ELSE NULL END
    )"""
    
    values_with_extra = [v + (v[10],) for v in upsert_values]
    
    result = execute_values(
        cur, upsert_query, values_with_extra,
        template=template, fetch=True
    )
    
    inserted = sum(1 for row in result if row[0])
    updated = len(result) - inserted
    
    return inserted, updated, llm_errors, duplicates_removed


# =============================================================================
# MAIN PROCESSING
# =============================================================================

def process_companies_batch(file):
    """Main function - process companies in batches."""
    complete_file_path = f"clay_company_100/{file}"
    print("🚀 Starting Company Upsert with HYBRID Popularity Index")
    print(f"⏰ Start time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📄 CSV file: {complete_file_path}")
    print(f"👤 User ID: {USER_ID}")
    print(f"📦 Batch size: {BATCH_SIZE}")
    print(f"🔧 Mode: {LOAD_MODE.upper()}")
    print(f"📊 Popularity Index Range: [{POPULARITY_INDEX_MIN}, {POPULARITY_INDEX_MAX}]")
    print("=" * 70)
    
    # Read CSV
    companies = read_csv_data(complete_file_path)
    if not companies:
        print("❌ No companies found in CSV")
        return

    total_rows = len(companies)
    # Connect to DB
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Get existing companies for incremental mode
    existing_companies = {}
    if LOAD_MODE == 'incremental':
        existing_companies = get_existing_companies(cur, USER_ID)
        print(f"📊 Found {len(existing_companies)} existing companies in DB")
    
    print(f"\n📊 Popularity Index Configuration:")
    print(f"   - Range: [{POPULARITY_INDEX_MIN}, {POPULARITY_INDEX_MAX}]")
    if LOAD_MODE == 'initial':
        print(f"   - Mode: INITIAL (position-based)")
        print(f"   - Formula: {POPULARITY_INDEX_MIN} + ((N - row) / (N - 1)) * {POPULARITY_INDEX_MAX - POPULARITY_INDEX_MIN}")
        print(f"   - Row 1: {calculate_position_based_index(1, total_rows):.6f}")
        print(f"   - Row {total_rows}: {calculate_position_based_index(total_rows, total_rows):.6f}")
    else:
        print(f"   - Mode: INCREMENTAL (attribute-based)")
        print(f"   - New companies scored by: brand, revenue, size, market leadership")
        print(f"   - Subsidiaries: parent_score - 0.00001")
    print(f"   - Sort: ORDER BY popularity_index DESC")
    
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
            
            # Enrich with LLM
            batch_start_time = time.time()
            enriched_data = enrich_companies_parallel(batch_companies, LOAD_MODE)
            llm_time = time.time() - batch_start_time
            
            # Batch upsert
            db_start_time = time.time()
            try:
                inserted, updated, llm_errors, duplicates = batch_upsert_companies(
                    cur, USER_ID, enriched_data, total_rows, LOAD_MODE, existing_companies
                )
                conn.commit()
            except Exception as e:
                print(f"❌ Error in batch upsert: {e}")
                pass
            db_time = time.time() - db_start_time
            
            # Update existing_companies for subsequent batches in incremental mode
            if LOAD_MODE == 'incremental':
                for company, _ in enriched_data:
                    name_lower = company['name'].strip().lower()
                    if name_lower not in existing_companies:
                        # Approximate the index for tracking
                        existing_companies[name_lower] = 15.0
            
            total_inserted += inserted
            total_updated += updated
            total_llm_errors += llm_errors
            total_duplicates += duplicates
            
            print(f"    ✅ Inserted: {inserted}, Updated: {updated}, LLM errors: {llm_errors}", end="")
            if duplicates > 0:
                print(f", Duplicates: {duplicates}")
            else:
                print()
            print(f"    ⏱️  LLM: {llm_time:.1f}s, DB: {db_time:.2f}s")
            
            elapsed = time.time() - start_time
            if batch_end > 0:
                rate = batch_end / elapsed
                remaining = total_count - batch_end
                eta_min = (remaining / rate) / 60 if rate > 0 else 0
                print(f"    📈 Rate: {rate:.1f}/sec, ETA: {eta_min:.1f} min\n")
        
        total_time = time.time() - start_time
        print("\n" + "=" * 70)
        print("✅ Processing Complete!")
        print(f"   - Total companies: {total_count}")
        print(f"   - Inserted: {total_inserted}")
        print(f"   - Updated: {total_updated}")
        print(f"   - Duplicates skipped: {total_duplicates}")
        print(f"   - LLM errors: {total_llm_errors}")
        print(f"   - Total time: {total_time:.1f}s ({total_time/60:.1f} min)")
        print("=" * 70)
        print("\n📌 To display in correct order:")
        print("   SELECT * FROM company_master ORDER BY popularity_index DESC;")
        
    except KeyboardInterrupt:
        print("\n⚠️ Interrupted by user")
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


# =============================================================================
# TEST MODE
# =============================================================================

def test_popularity_calculations():
    """Test both position-based and attribute-based calculations."""
    print("\n🧪 Testing HYBRID Popularity Index System")
    print(f"📊 Configured Range: [{POPULARITY_INDEX_MIN}, {POPULARITY_INDEX_MAX}]")
    print("=" * 70)
    
    # Test position-based (initial load)
    print("\n1️⃣  POSITION-BASED (Initial Load)")
    print("-" * 40)
    total = 3800
    test_rows = [1, 2, 10, 100, 1000, 3799, 3800]
    for row in test_rows:
        idx = calculate_position_based_index(row, total)
        print(f"   Row {row:>5}: popularity_index = {idx:.6f}")
    
    # Test attribute-based (new companies)
    print("\n2️⃣  ATTRIBUTE-BASED (New Companies)")
    print("-" * 40)
    
    test_companies = [
        ("Google-level", 10, 500_000_000_000, 10, 10, 10, 10, True),
        ("Large Enterprise", 7, 5_000_000_000, 8, 7, 8, 7, True),
        ("Mid-size Tech", 5, 100_000_000, 6, 5, 5, 6, True),
        ("Small Startup", 2, 5_000_000, 3, 2, 2, 4, True),
        ("Unknown Local", 1, 1_000_000, 2, 1, 1, 2, True),
        ("Non-profit", 3, 10_000_000, 4, 3, 3, 3, False),
    ]
    
    for name, brand, revenue, size, market, global_p, tech, commercial in test_companies:
        idx = calculate_attribute_based_index(brand, revenue, size, market, global_p, tech, commercial)
        print(f"   {name:<20}: popularity_index = {idx:.6f}")
    
    # Test subsidiary logic
    print("\n3️⃣  SUBSIDIARY LOGIC")
    print("-" * 40)
    parent_index = POPULARITY_INDEX_MAX - 0.25  # Near top of range
    print(f"   Parent company: popularity_index = {parent_index:.6f}")
    for i in range(1, 4):
        sub_idx = calculate_subsidiary_index(parent_index, i)
        print(f"   Subsidiary #{i}:   popularity_index = {sub_idx:.6f}")
    
    print("\n" + "=" * 70)
    print(f"✅ All indices fall within [{POPULARITY_INDEX_MIN}, {POPULARITY_INDEX_MAX}] range")
    print("✅ Higher index = appears first when sorted DESC")
    print("✅ Set POPULARITY_INDEX_MIN and POPULARITY_INDEX_MAX env vars to customize")
    print("=" * 70)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        test_popularity_calculations()
    else:
        for file in CSV_ALL_FILES:
            process_companies_batch(file)