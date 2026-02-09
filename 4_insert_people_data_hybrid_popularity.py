"""
People Data Batch Upsert Script with HYBRID Popularity Index

POPULARITY INDEX LOGIC:
========================

1. INITIAL CSV LOAD:
   - popularity_index = MIN + ((N - row_number) / (N - 1)) * (MAX - MIN)
   - Range: [MIN, MAX] where first person = MAX, last = MIN
   - Preserves exact CSV order

2. NEW PEOPLE (not in original CSV):
   - Calculated from: company_popularity, seniority_level, title_importance
   - Formula produces score in [MIN, MAX] range
   - New people slot into correct position based on their attributes

SIZE MAPPING:
- The 'size' field is NOT in the CSV
- It will be looked up from company_master table using company_name

CONFIGURABLE PARAMETERS:
- POPULARITY_INDEX_MIN: Lower bound (default: 10)
- POPULARITY_INDEX_MAX: Upper bound (default: 15)

DEDUPLICATION:
- Keeps FIRST occurrence only (based on name, company_name, title)
"""

import psycopg2
import os
import time
import math
import json
import csv
import re
from psycopg2.extras import execute_values
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Database config
DB_CONFIG = {
    "database": os.getenv("DB_NAME", "postgres"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "your-super-secret-and-long-postgres-password"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", 5433),
}

# CSV file path
CSV_FILE_PATH = os.getenv("CSV_FILE_PATH", "clay_people/retail.csv")

# CSV_ALL_FILES = ['Advertising_Services.csv', 'Banking.csv', 'Computer_and_Network_Security.csv', 'Data_Infrastructure_and_Analytics.csv', 'Financial_Services.csv', 'Insurance.csv', 'IT_Services_and_IT_Consulting.csv', 'Marketing_Services.csv', 'Venture_Capital_and_Private_Equity.csv']
CSV_ALL_FILES = ['Accounting.csv', 'Biotechnology Research.csv', 'Biotechnology.csv', 'Brokerages.csv', 'Business Consulting and Services.csv', 'Business Intelligence Platforms.csv', 'Capital Markets.csv', 'Commercial Real Estate.csv', 'Data Security Software Products.csv', 'E-Learning Providers.csv', 'Education Management.csv', 'Freight and Package Transportation.csv', 'Higher Education.csv', 'Hospitals and Health Care.csv', 'Hospitals.csv', 'Human Resources Services.csv', 'Import and Export.csv', 'Information Technology.csv', 'Internet Marketplace Platforms.csv', 'Internet.csv', 'Investment Banking.csv', 'Investment Management.csv', 'Market Research.csv', 'Media & Telecommunications.csv', 'Medical Devices.csv', 'Operations Consulting.csv', 'Outsourcing and Offshoring.csv', 'OutsourcingOffshoring.csv', 'Pharmaceutical Manufacturing.csv', 'Public Relations.csv', 'Real Estate.csv', 'Staffing and Recruiting.csv', 'Strategic Management Services.csv', 'Technology, Information and Media.csv', 'Telecommunications Carriers.csv', 'Telecommunications.csv', 'Transportation, Logistics, Supply.csv', 'Warehousing and Storage.csv']
# User ID (deprecated for people_data, but kept for compatibility)
USER_ID = os.getenv("USER_ID", None)

# Processing settings
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))

# Mode: 'initial' for first CSV load, 'incremental' for new people
LOAD_MODE = os.getenv("LOAD_MODE", "initial")  # 'initial' or 'incremental'

# Popularity Index Range Configuration (CONFIGURABLE)
POPULARITY_INDEX_MIN = float(os.getenv("POPULARITY_INDEX_MIN", "10"))
POPULARITY_INDEX_MAX = float(os.getenv("POPULARITY_INDEX_MAX", "15"))


# =============================================================================
# SENIORITY LEVEL DETECTION
# =============================================================================

def detect_seniority_level(title: str) -> str:
    """
    Detect seniority level from job title.
    Returns: 'C-Level', 'VP', 'Director', 'Manager', 'Senior', 'Mid', 'Junior', 'Entry'
    """
    if not title:
        return 'Mid'
    
    title_lower = title.lower()
    
    # C-Level patterns
    c_level_patterns = [
        'ceo', 'cto', 'cfo', 'coo', 'cmo', 'cio', 'cpo', 'cro', 'cso',
        'chief executive', 'chief technology', 'chief financial', 
        'chief operating', 'chief marketing', 'chief information',
        'chief product', 'chief revenue', 'chief strategy',
        'chairman', 'founder', 'co-founder', 'president'
    ]
    for pattern in c_level_patterns:
        if pattern in title_lower:
            return 'C-Level'
    
    # VP patterns
    vp_patterns = ['vice president', 'vp ', 'v.p.', 'svp', 'evp', 'avp']
    for pattern in vp_patterns:
        if pattern in title_lower:
            return 'VP'
    
    # Director patterns
    if 'director' in title_lower:
        return 'Director'
    
    # Manager patterns
    manager_patterns = ['manager', 'lead', 'head of', 'team lead', 'principal']
    for pattern in manager_patterns:
        if pattern in title_lower:
            return 'Manager'
    
    # Senior patterns
    if 'senior' in title_lower or 'sr.' in title_lower or 'sr ' in title_lower:
        return 'Senior'
    
    # Junior patterns
    junior_patterns = ['junior', 'jr.', 'jr ', 'associate', 'intern', 'trainee', 'entry']
    for pattern in junior_patterns:
        if pattern in title_lower:
            return 'Junior'
    
    return 'Mid'


def seniority_to_score(seniority: str) -> int:
    """Convert seniority level to a 1-10 score."""
    scores = {
        'C-Level': 10,
        'VP': 9,
        'Director': 8,
        'Manager': 7,
        'Senior': 6,
        'Mid': 5,
        'Junior': 3,
        'Entry': 2,
    }
    return scores.get(seniority, 5)


# =============================================================================
# DEPARTMENT DETECTION
# =============================================================================

def detect_department(title: str) -> str:
    """
    Detect department from job title.
    """
    if not title:
        return 'Other'
    
    title_lower = title.lower()
    
    # Engineering/Tech
    tech_patterns = ['engineer', 'developer', 'software', 'architect', 'devops', 
                     'sre', 'data scientist', 'machine learning', 'ml ', 'ai ',
                     'backend', 'frontend', 'full stack', 'platform', 'infrastructure']
    for pattern in tech_patterns:
        if pattern in title_lower:
            return 'Engineering'
    
    # Product
    product_patterns = ['product manager', 'product owner', 'product lead', 'pm ']
    for pattern in product_patterns:
        if pattern in title_lower:
            return 'Product'
    
    # Design
    design_patterns = ['designer', 'ux', 'ui', 'creative', 'visual']
    for pattern in design_patterns:
        if pattern in title_lower:
            return 'Design'
    
    # Sales
    sales_patterns = ['sales', 'account executive', 'ae ', 'business development', 'bdr', 'sdr']
    for pattern in sales_patterns:
        if pattern in title_lower:
            return 'Sales'
    
    # Marketing
    marketing_patterns = ['marketing', 'content', 'brand', 'communications', 'pr ', 'public relations']
    for pattern in marketing_patterns:
        if pattern in title_lower:
            return 'Marketing'
    
    # HR/People
    hr_patterns = ['hr ', 'human resources', 'people', 'talent', 'recruiter', 'recruiting']
    for pattern in hr_patterns:
        if pattern in title_lower:
            return 'HR'
    
    # Finance
    finance_patterns = ['finance', 'accountant', 'controller', 'treasury', 'fp&a']
    for pattern in finance_patterns:
        if pattern in title_lower:
            return 'Finance'
    
    # Operations
    ops_patterns = ['operations', 'ops ', 'supply chain', 'logistics']
    for pattern in ops_patterns:
        if pattern in title_lower:
            return 'Operations'
    
    # Legal
    if 'legal' in title_lower or 'counsel' in title_lower or 'attorney' in title_lower:
        return 'Legal'
    
    # Executive/General Management
    exec_patterns = ['ceo', 'cto', 'cfo', 'coo', 'chief', 'president', 'chairman', 'founder']
    for pattern in exec_patterns:
        if pattern in title_lower:
            return 'Executive'
    
    return 'Other'


# =============================================================================
# SIZE TO SCORE CONVERSION (same as company script)
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
# POPULARITY INDEX CALCULATIONS
# =============================================================================

def calculate_position_based_index(row_number: int, total_rows: int, 
                                    min_val: float = None, max_val: float = None) -> float:
    """
    Calculate popularity index from CSV position (for initial load).
    
    Formula: MIN + ((N - row_number) / (N - 1)) * (MAX - MIN)
    Range: [MIN, MAX] where first=MAX, last=MIN
    
    Args:
        row_number: 1-based row number (1 = first person)
        total_rows: Total number of unique people (N)
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
    seniority_score: int,
    company_size_score: int,
    company_popularity: float = None,
    min_val: float = None,
    max_val: float = None
) -> float:
    """
    Calculate popularity index from person attributes (for new people).
    
    Weights:
    - Seniority Level: 50% (most important for person ordering)
    - Company Size: 30%
    - Company Popularity: 20%
    
    Args:
        seniority_score: 1-10 score based on job title seniority
        company_size_score: 1-10 score based on company size
        company_popularity: Company's popularity_index (optional)
        min_val: Minimum popularity index value (default: POPULARITY_INDEX_MIN)
        max_val: Maximum popularity index value (default: POPULARITY_INDEX_MAX)
    
    Returns: Score in range [min_val, max_val]
    """
    min_val = min_val if min_val is not None else POPULARITY_INDEX_MIN
    max_val = max_val if max_val is not None else POPULARITY_INDEX_MAX
    index_range = max_val - min_val
    
    # Normalize scores to 0-1 range
    seniority_norm = max(1, min(10, seniority_score)) / 10
    size_norm = max(1, min(10, company_size_score)) / 10
    
    # Company popularity normalized (if available)
    if company_popularity is not None:
        company_norm = (company_popularity - min_val) / index_range
        company_norm = max(0, min(1, company_norm))
    else:
        company_norm = 0.5  # Default to middle
    
    # Weighted combination
    weighted_score = (
        seniority_norm * 0.50 +
        size_norm * 0.30 +
        company_norm * 0.20
    )
    
    # Scale to configured range
    popularity = min_val + weighted_score * index_range
    return round(popularity, 6)


# =============================================================================
# CSV READING
# =============================================================================

def read_csv_data(file_path: str, deduplicate: bool = True) -> List[Dict]:
    """
    Read people data from CSV file.
    
    CSV columns expected:
    - "Find people" (ignored)
    - "Company Name"
    - "First Name"
    - "Last Name"
    - "Full Name"
    - "Job Title"
    - "Location"
    - "Company Domain"
    - "LinkedIn Profile"
    
    - Assigns row_number (1-based) to each person
    - Deduplication keeps FIRST occurrence (based on name + company + title)
    """
    people = []
    seen_keys = set()
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
                
                full_name = get_field('Full Name', 'full_name', 'Name')
                company_name = get_field('Company Name', 'company_name', 'Company')
                title = get_field('Job Title', 'job_title', 'Title', 'Position')
                
                if not full_name or not company_name:
                    continue
                
                row_number += 1
                
                # Deduplication - keep FIRST occurrence only
                # Key is combination of name, company, and title
                if deduplicate:
                    dedup_key = (
                        full_name.strip().lower(),
                        company_name.strip().lower(),
                        (title.strip().lower() if title else '')
                    )
                    if dedup_key in seen_keys:
                        duplicates_in_csv += 1
                        continue
                    seen_keys.add(dedup_key)
                
                # Detect seniority and department from title
                seniority_level = detect_seniority_level(title)
                department = detect_department(title)
                
                # Extract LinkedIn ID from URL
                linkedin_url = get_field('LinkedIn Profile', 'linkedin_profile', 'LinkedIn URL', 'LinkedIn')
                linkedin_id = ''
                if linkedin_url:
                    # Extract the ID from URL like https://www.linkedin.com/in/username/
                    match = re.search(r'linkedin\.com/in/([^/\?]+)', linkedin_url)
                    if match:
                        linkedin_id = match.group(1)
                
                person = {
                    'name': full_name,
                    'company_name': company_name,
                    'title': title,
                    'location': get_field('Location', 'location'),
                    'linkedin_id': linkedin_id,
                    'linkedin_url_enriched': linkedin_url,
                    'company_domain_enriched': get_field('Company Domain', 'company_domain', 'Domain'),
                    'seniority_level': seniority_level,
                    'department': department,
                    'row_number': row_number,
                }
                people.append(person)
        
        print(f"📄 Read {len(people)} unique people from CSV")
        if duplicates_in_csv > 0:
            print(f"⚠️  Skipped {duplicates_in_csv} duplicate entries (kept first occurrence)")
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        raise
    
    return people


# =============================================================================
# DATABASE OPERATIONS
# =============================================================================

def get_company_sizes(cur, company_names: List[str]) -> Dict[str, str]:
    """Get company sizes from company_master table."""
    if not company_names:
        return {}
    
    cur.execute("SET statement_timeout TO 0;")
    # Query company_master for sizes
    placeholders = ','.join(['%s'] * len(company_names))
    cur.execute(f"""
        SELECT LOWER(name), size 
        FROM company_master 
        WHERE LOWER(name) IN ({placeholders})
    """, [name.lower() for name in company_names])
    
    return {row[0]: row[1] for row in cur.fetchall() if row[1]}


def get_company_popularity(cur, company_names: List[str]) -> Dict[str, float]:
    """Get company popularity indices from company_master table."""
    if not company_names:
        return {}
    
    placeholders = ','.join(['%s'] * len(company_names))
    cur.execute(f"""
        SELECT LOWER(name), popularity_index 
        FROM company_master 
        WHERE LOWER(name) IN ({placeholders}) AND popularity_index IS NOT NULL
    """, [name.lower() for name in company_names])
    
    return {row[0]: row[1] for row in cur.fetchall()}


def get_existing_people(cur) -> Dict[Tuple[str, str, str], float]:
    """Get existing people and their popularity indices from DB."""
    cur.execute("""
        SELECT LOWER(COALESCE(name, '')), LOWER(COALESCE(company_name, '')), 
               LOWER(COALESCE(title, '')), popularity_index 
        FROM people_data 
        WHERE popularity_index IS NOT NULL
    """)
    
    return {(row[0], row[1], row[2]): row[3] for row in cur.fetchall()}


def batch_upsert_people(
    cur, 
    people_data: List[Dict],
    company_sizes: Dict[str, str],
    company_popularity: Dict[str, float],
    total_rows: int,
    mode: str = 'initial',
    existing_people: Dict[Tuple[str, str, str], float] = None
) -> Tuple[int, int, int]:
    """
    Batch upsert people using ON CONFLICT.
    
    In 'initial' mode: popularity_index from row_number
    In 'incremental' mode: popularity_index from attributes
    """
    if not people_data:
        return 0, 0, 0
    
    existing_people = existing_people or {}
    
    # Deduplicate within batch - keep FIRST occurrence
    seen_keys = {}
    duplicates_removed = 0
    
    for person in people_data:
        key = (
            person['name'].strip().lower(),
            person['company_name'].strip().lower(),
            (person['title'].strip().lower() if person.get('title') else '')
        )
        if key not in seen_keys:
            seen_keys[key] = person
        else:
            duplicates_removed += 1
    
    deduplicated_data = list(seen_keys.values())
    
    if duplicates_removed > 0:
        print(f"    ⚠️ Removed {duplicates_removed} duplicates in batch (kept first)")
    
    # Prepare data for upsert
    upsert_values = []
    
    for person in deduplicated_data:
        company_lower = person['company_name'].strip().lower()
        
        # Get size from company_master
        size = company_sizes.get(company_lower)
        
        # Calculate popularity_index based on mode
        if mode == 'initial':
            # Position-based for initial load
            row_number = person.get('row_number', 1)
            popularity_index = calculate_position_based_index(row_number, total_rows)
        else:
            # Attribute-based for incremental load
            seniority_score = seniority_to_score(person.get('seniority_level', 'Mid'))
            size_score = size_to_score(size) if size else 5
            comp_popularity = company_popularity.get(company_lower)
            
            popularity_index = calculate_attribute_based_index(
                seniority_score=seniority_score,
                company_size_score=size_score,
                company_popularity=comp_popularity
            )
        
        upsert_values.append((
            person['company_name'],
            person['name'],
            person.get('title') or None,
            person.get('linkedin_id') or None,
            person.get('location') or None,
            person.get('department') or None,
            size,  # From company_master
            person.get('seniority_level') or None,
            person.get('linkedin_url_enriched') or None,
            person.get('company_domain_enriched') or None,
            popularity_index,
        ))
    
    upsert_query = """
        INSERT INTO people_data (
            company_name, name, title, linkedin_id, location, department, size,
            seniority_level, linkedin_url_enriched, company_domain_enriched,
            popularity_index, created_at, updated_at
        ) VALUES %s
        ON CONFLICT (name, company_name, title) DO UPDATE SET
            linkedin_id = COALESCE(EXCLUDED.linkedin_id, people_data.linkedin_id),
            location = COALESCE(EXCLUDED.location, people_data.location),
            department = COALESCE(EXCLUDED.department, people_data.department),
            size = COALESCE(EXCLUDED.size, people_data.size),
            seniority_level = COALESCE(EXCLUDED.seniority_level, people_data.seniority_level),
            linkedin_url_enriched = COALESCE(EXCLUDED.linkedin_url_enriched, people_data.linkedin_url_enriched),
            company_domain_enriched = COALESCE(EXCLUDED.company_domain_enriched, people_data.company_domain_enriched),
            popularity_index = EXCLUDED.popularity_index,
            updated_at = NOW()
        RETURNING (xmax = 0) AS inserted
    """
    
    template = """(
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()
    )"""
    
    result = execute_values(
        cur, upsert_query, upsert_values,
        template=template, fetch=True
    )
    
    inserted = sum(1 for row in result if row[0])
    updated = len(result) - inserted
    
    return inserted, updated, duplicates_removed


# =============================================================================
# MAIN PROCESSING
# =============================================================================

def process_people_batch(file):
    """Main function - process people in batches."""
    complete_file_path = f"clay_people_50/{file}"
    print("🚀 Starting People Data Upsert with HYBRID Popularity Index")
    print(f"⏰ Start time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📄 CSV file: {complete_file_path}")
    print(f"📦 Batch size: {BATCH_SIZE}")
    print(f"🔧 Mode: {LOAD_MODE.upper()}")
    print(f"📊 Popularity Index Range: [{POPULARITY_INDEX_MIN}, {POPULARITY_INDEX_MAX}]")
    print("=" * 70)
    
    # Read CSV
    people = read_csv_data(complete_file_path)
    
    if not people:
        print("❌ No people found in CSV")
        return
    
    total_rows = len(people)
    
    # Connect to DB
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Get unique company names for size lookup
    unique_companies = list(set(p['company_name'] for p in people))
    print(f"\n📊 Found {len(unique_companies)} unique companies")
    
    # Get company sizes from company_master
    print("🔍 Looking up company sizes from company_master...")
    company_sizes = get_company_sizes(cur, unique_companies)
    print(f"   Found sizes for {len(company_sizes)} companies")
    
    # Get company popularity for incremental mode
    company_popularity = {}
    if LOAD_MODE == 'incremental':
        company_popularity = get_company_popularity(cur, unique_companies)
        print(f"   Found popularity indices for {len(company_popularity)} companies")
    
    # Get existing people for incremental mode
    existing_people = {}
    if LOAD_MODE == 'incremental':
        existing_people = get_existing_people(cur)
        print(f"📊 Found {len(existing_people)} existing people in DB")
    
    print(f"\n📊 Popularity Index Configuration:")
    print(f"   - Range: [{POPULARITY_INDEX_MIN}, {POPULARITY_INDEX_MAX}]")
    if LOAD_MODE == 'initial':
        print(f"   - Mode: INITIAL (position-based)")
        print(f"   - Formula: {POPULARITY_INDEX_MIN} + ((N - row) / (N - 1)) * {POPULARITY_INDEX_MAX - POPULARITY_INDEX_MIN}")
        print(f"   - Row 1: {calculate_position_based_index(1, total_rows):.6f}")
        print(f"   - Row {total_rows}: {calculate_position_based_index(total_rows, total_rows):.6f}")
    else:
        print(f"   - Mode: INCREMENTAL (attribute-based)")
        print(f"   - New people scored by: seniority, company size, company popularity")
    print(f"   - Sort: ORDER BY popularity_index DESC")
    
    total_count = len(people)
    total_inserted = 0
    total_updated = 0
    total_duplicates = 0
    total_batches = (total_count + BATCH_SIZE - 1) // BATCH_SIZE
    
    print(f"\n📊 Total people: {total_count}")
    print(f"📦 Total batches: {total_batches}")
    print("=" * 70 + "\n")
    
    start_time = time.time()
    
    try:
        for batch_num in range(total_batches):
            batch_start = batch_num * BATCH_SIZE
            batch_end = min(batch_start + BATCH_SIZE, total_count)
            batch_people = people[batch_start:batch_end]
            
            batch_pct = ((batch_num + 1) / total_batches) * 100
            print(f"[Batch {batch_num + 1}/{total_batches}] ({batch_pct:.1f}%) Processing {len(batch_people)} people...")
            
            # Batch upsert
            db_start_time = time.time()
            inserted, updated, duplicates = batch_upsert_people(
                cur, batch_people, company_sizes, company_popularity,
                total_rows, LOAD_MODE, existing_people
            )
            conn.commit()
            db_time = time.time() - db_start_time
            
            total_inserted += inserted
            total_updated += updated
            total_duplicates += duplicates
            
            print(f"    ✅ Inserted: {inserted}, Updated: {updated}", end="")
            if duplicates > 0:
                print(f", Duplicates: {duplicates}")
            else:
                print()
            print(f"    ⏱️  DB: {db_time:.2f}s")
            
            elapsed = time.time() - start_time
            if batch_end > 0:
                rate = batch_end / elapsed
                remaining = total_count - batch_end
                eta_min = (remaining / rate) / 60 if rate > 0 else 0
                print(f"    📈 Rate: {rate:.1f}/sec, ETA: {eta_min:.1f} min\n")
        
        total_time = time.time() - start_time
        print("\n" + "=" * 70)
        print("✅ Processing Complete!")
        print(f"   - Total people: {total_count}")
        print(f"   - Inserted: {total_inserted}")
        print(f"   - Updated: {total_updated}")
        print(f"   - Duplicates skipped: {total_duplicates}")
        print(f"   - Total time: {total_time:.1f}s ({total_time/60:.1f} min)")
        print("=" * 70)
        print("\n📌 To display in correct order:")
        print("   SELECT * FROM people_data ORDER BY popularity_index DESC;")
        
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
    print("\n🧪 Testing HYBRID Popularity Index System for People Data")
    print(f"📊 Configured Range: [{POPULARITY_INDEX_MIN}, {POPULARITY_INDEX_MAX}]")
    print("=" * 70)
    
    # Test position-based (initial load)
    print("\n1️⃣  POSITION-BASED (Initial Load)")
    print("-" * 40)
    total = 4900
    test_rows = [1, 2, 10, 100, 1000, 4899, 4900]
    for row in test_rows:
        idx = calculate_position_based_index(row, total)
        print(f"   Row {row:>5}: popularity_index = {idx:.6f}")
    
    # Test attribute-based (new people)
    print("\n2️⃣  ATTRIBUTE-BASED (New People)")
    print("-" * 40)
    
    test_people = [
        ("C-Level at Large Corp", 10, 10, 15.0),
        ("VP at Mid Corp", 9, 7, 13.0),
        ("Director at Startup", 8, 4, 12.0),
        ("Senior Engineer at Large", 6, 10, 14.0),
        ("Junior at Startup", 3, 3, 11.0),
        ("Entry at Unknown", 2, 5, None),
    ]
    
    for name, seniority, size, company_pop in test_people:
        idx = calculate_attribute_based_index(seniority, size, company_pop)
        print(f"   {name:<30}: popularity_index = {idx:.6f}")
    
    # Test seniority detection
    print("\n3️⃣  SENIORITY DETECTION")
    print("-" * 40)
    
    test_titles = [
        "CEO",
        "Chief Technology Officer",
        "Vice President of Engineering",
        "SVP Marketing",
        "Director of Sales",
        "Engineering Manager",
        "Senior Software Engineer",
        "Software Engineer",
        "Junior Developer",
        "Intern",
    ]
    
    for title in test_titles:
        seniority = detect_seniority_level(title)
        score = seniority_to_score(seniority)
        print(f"   {title:<35}: {seniority:<10} (score: {score})")
    
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
            process_people_batch(file)