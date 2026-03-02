"""
Company Master Batch Upsert Script with LLM-Based Popularity Index (0-100,000)

POPULARITY INDEX LOGIC:
========================

ALL COMPANIES (both initial and incremental):
  - LLM provides: brand_recognition, market_leader, global_presence, 
    tech_innovation scores (1-10) + estimated_revenue_usd + is_commercial
  - Formula produces score in [0, 100,000] range using weighted components:
    * Revenue (log scale):      0 - 30,000 points
    * Brand Recognition:        0 - 30,000 points  
    * Market Leadership:        0 - 20,000 points
    * Global Presence:          0 - 10,000 points
    * Tech Innovation:          0 - 10,000 points
  - Top companies (Apple, Google, Amazon): ~90,000 - 100,000
  - Large enterprises: ~40,000 - 70,000
  - Mid-size companies: ~15,000 - 35,000
  - Small/local companies: ~1,000 - 10,000
  - Non-commercial entities: 10

SUBSIDIARIES:
  - Score = parent_score - 1 (appears just below parent)

CONFIGURABLE PARAMETERS:
- POPULARITY_INDEX_MIN: Lower bound (default: 0)
- POPULARITY_INDEX_MAX: Upper bound (default: 100000)

DEDUPLICATION:
- Keeps FIRST occurrence only

LLM ENRICHMENT:
- Enriches revenue_range, employee_count, AND popularity scoring metrics for ALL companies
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
CSV_ALL_FILES = ['Advertising_Services.csv', 'Banking.csv', 'Computer_and_Network_Security.csv', 'Data_Infrastructure_and_Analytics.csv', 'Financial_Services.csv', 'Insurance.csv', 'IT_Services_and_IT_Consulting.csv', 'Marketing_Services.csv', 'retail.csv', 'software_development.csv', 'Venture_Capital_and_Private_Equity.csv']
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
POPULARITY_INDEX_MIN = float(os.getenv("POPULARITY_INDEX_MIN", "0"))
POPULARITY_INDEX_MAX = float(os.getenv("POPULARITY_INDEX_MAX", "100000"))


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

def calculate_popularity_index(
    revenue_estimate: float,
    brand_score: int,
    market_leader_score: int,
    global_presence_score: int,
    tech_innovation_score: int,
    is_commercial: bool
) -> float:
    """
    Calculate popularity index on a 0-100,000 scale.
    
    Components (total = 100,000 max):
      - Revenue (log scale):      0 - 30,000 points
      - Brand Recognition:        0 - 30,000 points
      - Market Leadership:        0 - 20,000 points
      - Global Presence:          0 - 10,000 points
      - Tech Innovation:          0 - 10,000 points
    
    Examples:
      Apple/Google/Amazon (all 10s, $300B+ rev) → ~95,000+
      Large enterprise (7-8 scores, $5B rev)    → ~50,000-65,000
      Mid-size company (5-6 scores, $100M rev)  → ~20,000-35,000
      Small local company (2-3 scores, $5M rev) → ~3,000-8,000
      Non-commercial entity                     → 10
    """
    if not is_commercial:
        return 10.0
    
    # --- REVENUE POINTS (0 - 30,000) ---
    if revenue_estimate <= 0:
        revenue_points = 0.0
    else:
        log_rev = math.log10(max(revenue_estimate, 1))
        if log_rev <= 6:       # Under $1M
            revenue_points = (log_rev / 6) * 3000
        elif log_rev <= 9:     # $1M - $1B
            revenue_points = 3000 + ((log_rev - 6) / 3) * 12000
        elif log_rev <= 11:    # $1B - $100B
            revenue_points = 15000 + ((log_rev - 9) / 2) * 10000
        elif log_rev <= 11.7:  # $100B - $500B
            revenue_points = 25000 + ((log_rev - 11) / 0.7) * 4000
        else:                  # $500B+
            revenue_points = 29000 + min(1000, (log_rev - 11.7) * 1000)
    revenue_points = min(30000, max(0, revenue_points))
    
    # --- BRAND RECOGNITION POINTS (0 - 30,000) ---
    brand_norm = max(1, min(10, brand_score))
    if brand_norm <= 3:
        brand_points = (brand_norm / 3) * 3000
    elif brand_norm <= 6:
        brand_points = 3000 + ((brand_norm - 3) / 3) * 7000
    elif brand_norm <= 8:
        brand_points = 10000 + ((brand_norm - 6) / 2) * 10000
    else:
        brand_points = 20000 + ((brand_norm - 8) / 2) * 10000
    
    # --- MARKET LEADER POINTS (0 - 20,000) ---
    market_norm = max(1, min(10, market_leader_score))
    if market_norm <= 3:
        market_points = (market_norm / 3) * 2000
    elif market_norm <= 6:
        market_points = 2000 + ((market_norm - 3) / 3) * 4000
    elif market_norm <= 8:
        market_points = 6000 + ((market_norm - 6) / 2) * 6000
    else:
        market_points = 12000 + ((market_norm - 8) / 2) * 8000
    
    # --- GLOBAL PRESENCE POINTS (0 - 10,000) ---
    global_norm = max(1, min(10, global_presence_score))
    global_points = (global_norm / 10) * 10000
    
    # --- TECH INNOVATION POINTS (0 - 10,000) ---
    tech_norm = max(1, min(10, tech_innovation_score))
    tech_points = (tech_norm / 10) * 10000
    
    # --- TOTAL ---
    raw_score = revenue_points + brand_points + market_points + global_points + tech_points
    
    # Ensure within [0, 100000]
    final_score = max(0, min(100000.0, round(raw_score, 2)))
    return final_score


def calculate_position_based_index(row_number: int, total_rows: int, 
                                    min_val: float = None, max_val: float = None) -> float:
    """
    Fallback: Calculate popularity index from CSV position.
    Only used when LLM enrichment fails and no attribute data available.
    
    Formula: MIN + ((N - row_number) / (N - 1)) * (MAX - MIN)
    """
    min_val = min_val if min_val is not None else POPULARITY_INDEX_MIN
    max_val = max_val if max_val is not None else POPULARITY_INDEX_MAX
    
    if total_rows <= 1:
        return max_val
    
    index_range = max_val - min_val
    popularity = min_val + ((total_rows - row_number) / (total_rows - 1)) * index_range
    return round(popularity, 2)


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
    Calculate popularity index from company attributes.
    Delegates to the main calculate_popularity_index function.
    size_score is not used directly (revenue covers company size impact).
    """
    return calculate_popularity_index(
        revenue_estimate,
        brand_recognition_score,
        market_leader_score,
        global_presence_score,
        tech_innovation_score,
        is_commercial
    )


def calculate_subsidiary_index(parent_index: float, subsidiary_order: int = 1) -> float:
    """
    Calculate popularity index for a subsidiary.
    Subsidiaries appear just below their parent company.
    Each subsidiary gets score decremented by 1 per order.
    """
    return round(max(0, parent_index - (1 * subsidiary_order)), 2)


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
    """Build prompt for INITIAL load - gets revenue, employee count, AND popularity metrics."""
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

    2. **employee_count**: Estimated total number of employees as an integer

    3. **estimated_revenue_usd**: Annual revenue estimate in USD (number only, e.g., 5000000000)
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
    """Call LLM to get enrichment data including popularity scoring metrics."""
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
            
            # Validate required keys - always need full metrics now
            required_keys = [
                "revenue_range", "employee_count", "estimated_revenue_usd",
                "brand_recognition_score", "market_leader_score",
                "global_presence_score", "tech_innovation_score", "is_commercial_entity"
            ]
            
            if not all(key in metrics for key in required_keys):
                raise ValueError(f"Missing keys: {[k for k in required_keys if k not in metrics]}")
            
            # Fix revenue_range
            valid_ranges = ["$500B+", "$200B+", "$100B+", "$50B+", "$25B+", "$10B+",
                          "$5B+", "$1B+", "$500M+", "$100M+", "$50M+", "<$50M"]
            if metrics["revenue_range"] not in valid_ranges:
                metrics["revenue_range"] = fix_revenue_range(metrics["revenue_range"])
            
            # Calculate popularity index (1 - 100,000 scale)
            popularity_index = calculate_popularity_index(
                float(metrics["estimated_revenue_usd"]),
                int(metrics["brand_recognition_score"]),
                int(metrics["market_leader_score"]),
                int(metrics["global_presence_score"]),
                int(metrics["tech_innovation_score"]),
                bool(metrics["is_commercial_entity"])
            )
            
            result = {
                'revenue_range': metrics["revenue_range"],
                'employee_count': int(metrics["employee_count"]),
                'estimated_revenue_usd': float(metrics["estimated_revenue_usd"]),
                'brand_recognition_score': int(metrics["brand_recognition_score"]),
                'market_leader_score': int(metrics["market_leader_score"]),
                'global_presence_score': int(metrics["global_presence_score"]),
                'tech_innovation_score': int(metrics["tech_innovation_score"]),
                'is_commercial_entity': bool(metrics["is_commercial_entity"]),
                'popularity_index': popularity_index,
            }
            
            if mode == 'incremental':
                result['parent_company'] = metrics.get("parent_company")
            
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
        # Calculate popularity_index - always use LLM-based calculation
        if enrichment and 'popularity_index' in enrichment:
            if mode == 'incremental':
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
                        # Parent not found, use LLM-calculated score
                        popularity_index = enrichment['popularity_index']
                else:
                    # Not a subsidiary, use LLM-calculated score
                    popularity_index = enrichment['popularity_index']
            else:
                # Initial mode: use LLM-calculated popularity directly
                popularity_index = enrichment['popularity_index']
        else:
            # No enrichment data - fallback to position-based or size-based
            if mode == 'initial':
                row_number = company.get('row_number', 1)
                popularity_index = calculate_position_based_index(row_number, total_rows)
            else:
                size_score = size_to_score(company.get('size', ''))
                # Scale size_score (1-10) to a low range in 0-100000
                popularity_index = max(0, size_score * 1000)
        
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
    complete_file_path = f"clay_company/{file}"
    print("🚀 Starting Company Upsert with LLM-Based Popularity Index (0-100,000)")
    print(f"⏰ Start time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📄 CSV file: {complete_file_path}")
    print(f"👤 User ID: {USER_ID}")
    print(f"📦 Batch size: {BATCH_SIZE}")
    print(f"🔧 Mode: {LOAD_MODE.upper()}")
    print(f"📊 Popularity Index Range: [0, 100,000]")
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
    print(f"   - Range: [0, 100,000]")
    print(f"   - Mode: LLM-BASED (all companies scored by brand, revenue, market leadership)")
    if LOAD_MODE == 'initial':
        print(f"   - Load type: INITIAL")
    else:
        print(f"   - Load type: INCREMENTAL (subsidiaries scored below parent)")
    print(f"   - Fallback: position-based if LLM fails")
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
                        existing_companies[name_lower] = 50000.0
            
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
    """Test the popularity index calculations on 0-100,000 scale."""
    print("\n🧪 Testing Popularity Index System (0 - 100,000 scale)")
    print("=" * 70)
    
    # Test main popularity calculation
    print("\n1️⃣  LLM-BASED POPULARITY INDEX")
    print("-" * 50)
    
    test_companies = [
        ("Apple/Google/Amazon", 500_000_000_000, 10, 10, 10, 10, True),
        ("Microsoft/Meta",      300_000_000_000, 10, 9, 10, 10, True),
        ("Nike/Coca-Cola",       50_000_000_000, 9, 8, 9, 6, True),
        ("Salesforce/Adobe",     20_000_000_000, 8, 8, 9, 9, True),
        ("Large Enterprise",      5_000_000_000, 7, 7, 8, 7, True),
        ("Mid-size National",       500_000_000, 5, 5, 5, 5, True),
        ("Regional Company",        100_000_000, 4, 4, 4, 4, True),
        ("Small Startup",             5_000_000, 2, 2, 2, 5, True),
        ("Local Business",             1_000_000, 1, 1, 1, 1, True),
        ("Non-profit/NGO",            10_000_000, 4, 3, 3, 3, False),
    ]
    
    print(f"  {'Company':<25} {'Revenue':>15} {'Brand':>6} {'Mkt':>4} {'Glbl':>5} {'Tech':>5}  {'Index':>10}")
    print(f"  {'-'*25} {'-'*15} {'-'*6} {'-'*4} {'-'*5} {'-'*5}  {'-'*10}")
    
    for name, revenue, brand, market, glbl, tech, commercial in test_companies:
        idx = calculate_popularity_index(revenue, brand, market, glbl, tech, commercial)
        rev_str = f"${revenue:,.0f}"
        print(f"  {name:<25} {rev_str:>15} {brand:>6} {market:>4} {glbl:>5} {tech:>5}  {idx:>10,.2f}")
    
    # Test subsidiary logic
    print("\n2️⃣  SUBSIDIARY LOGIC")
    print("-" * 50)
    parent_index = 85000.0
    print(f"  Parent company:  popularity_index = {parent_index:,.2f}")
    for i in range(1, 4):
        sub_idx = calculate_subsidiary_index(parent_index, i)
        print(f"  Subsidiary #{i}:   popularity_index = {sub_idx:,.2f}")
    
    # Test fallback position-based
    print("\n3️⃣  FALLBACK: Position-Based (when LLM fails)")
    print("-" * 50)
    total = 3800
    test_rows = [1, 10, 100, 1000, 3800]
    for row in test_rows:
        idx = calculate_position_based_index(row, total)
        print(f"  Row {row:>5} of {total}: popularity_index = {idx:,.2f}")
    
    print("\n" + "=" * 70)
    print("✅ Scale: 0 (lowest) to 100,000 (highest)")
    print("✅ Top tech giants → 90,000+")
    print("✅ Small local companies → under 10,000")
    print("✅ 32M+ companies get proper spread for correct sorting")
    print("✅ Higher index = appears first when sorted DESC")
    print("=" * 70)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        test_popularity_calculations()
    else:
        for file in CSV_ALL_FILES:
            process_companies_batch(file)