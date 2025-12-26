#!/usr/bin/env python3
"""
OPTIMIZED Script to insert people data into PostgreSQL
Fixed for UNIQUE constraint on (name, company_name, title)
"""

import csv
import os
import sys
import argparse
import re
import time
from datetime import datetime
from typing import Optional, Dict

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import psycopg2
    from psycopg2.extras import execute_values, execute_batch
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)


# ============================================================
# CONFIGURATION
# ============================================================
ALLOWED_SIZES = [
    "10,001+ employees",
    "5,001-10,000 employees", 
    "1,001-5,000 employees",
    "501-1,000 employees",
    "201-500 employees",
    "51-200 employees",
    "11-50 employees",
    "1-10 employees",
]

# Columns to insert (excluding auto-generated 'id')
INSERT_COLUMNS = [
    'company_name', 'name', 'title', 'linkedin_id', 'location', 'department',
    'size', 'company_id', 'seniority_level', 'years_experience', 'skills',
    'education', 'profile_photo_url', 'email_address', 'phone_number',
    'last_updated_at', 'confidence_score', 'intent_signals', 'company_funding_stage',
    'job_change_date', 'technologies', 'enrichment_status', 'last_enriched_at',
    'work_email_enriched', 'linkedin_url_enriched', 'social_profiles_enriched',
    'person_highlights_enriched', 'company_domain_enriched', 'user_id',
    'created_at', 'updated_at', 'popularity_index'
]


def get_db_config() -> Dict[str, str]:
    return {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', '5432')),
        'database': os.getenv('DB_NAME', 'postgres'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', '')
    }


def normalize_size(employee_count_str: str, employee_size_str: str) -> Optional[str]:
    if not employee_count_str and not employee_size_str:
        return None
    
    try:
        count_str = str(employee_count_str).replace(',', '').strip()
        if count_str and count_str.isdigit():
            count = int(count_str)
            if count >= 10001: return "10,001+ employees"
            elif count >= 5001: return "5,001-10,000 employees"
            elif count >= 1001: return "1,001-5,000 employees"
            elif count >= 501: return "501-1,000 employees"
            elif count >= 201: return "201-500 employees"
            elif count >= 51: return "51-200 employees"
            elif count >= 11: return "11-50 employees"
            else: return "1-10 employees"
    except (ValueError, TypeError):
        pass
    
    size_str = str(employee_size_str).lower().strip() if employee_size_str else ""
    size_mapping = {
        '10000+': "10,001+ employees", '10001+': "10,001+ employees",
        '5000-10000': "5,001-10,000 employees", '5001-10000': "5,001-10,000 employees",
        '1000-5000': "1,001-5,000 employees", '1001-5000': "1,001-5,000 employees",
        '500-1000': "501-1,000 employees", '501-1000': "501-1,000 employees",
        '250-500': "201-500 employees", '201-500': "201-500 employees",
        '100-250': "51-200 employees", '51-200': "51-200 employees",
        '11-50': "11-50 employees", '1-10': "1-10 employees", '1-100': "51-200 employees",
    }
    
    clean_size = re.sub(r'\s+', '', size_str)
    clean_size = re.sub(r'employees?', '', clean_size)
    
    if clean_size in size_mapping:
        return size_mapping[clean_size]
    
    numbers = re.findall(r'\d+', size_str)
    if numbers:
        lower = int(numbers[0])
        if lower >= 10000: return "10,001+ employees"
        elif lower >= 5000: return "5,001-10,000 employees"
        elif lower >= 1000: return "1,001-5,000 employees"
        elif lower >= 500: return "501-1,000 employees"
        elif lower >= 200: return "201-500 employees"
        elif lower >= 50: return "51-200 employees"
        elif lower >= 10: return "11-50 employees"
        else: return "1-10 employees"
    
    return None


def infer_seniority_level(title: str) -> Optional[str]:
    if not title:
        return None
    title_lower = title.lower()
    
    if any(x in title_lower for x in ['ceo', 'cfo', 'cto', 'coo', 'cmo', 'chief', 'president', 'founder']):
        return 'C-Suite'
    elif any(x in title_lower for x in ['evp', 'svp', 'executive vice president', 'senior vice president']):
        return 'VP'
    elif any(x in title_lower for x in ['vice president', ' vp', 'vp ']):
        return 'VP'
    elif any(x in title_lower for x in ['director', 'head of']):
        return 'Director'
    elif 'manager' in title_lower:
        return 'Manager'
    elif any(x in title_lower for x in ['senior', 'sr.', 'lead', 'principal']):
        return 'Senior'
    return 'Individual Contributor'


def process_csv_row(row: dict) -> Dict:
    """Process a single CSV row."""
    first_name = row.get('First Name', '').strip()
    last_name = row.get('Last Name', '').strip()
    full_name = f"{first_name} {last_name}".strip()
    
    # Build location
    city = row.get('City', '').strip()
    state = row.get('State', '').strip()
    country = row.get('Country', '').strip()
    location_parts = [p for p in [city, state, country] if p]
    location = ', '.join(location_parts) if location_parts else None
    
    # Size normalization
    size = normalize_size(
        row.get('# Employees', ''),
        row.get('Employee size', '')
    )
    
    # LinkedIn
    linkedin_url = row.get('Person Linkedin Url', '').strip()
    linkedin_id = None
    if linkedin_url:
        parts = linkedin_url.rstrip('/').split('/')
        if 'in' in parts:
            idx = parts.index('in')
            if idx + 1 < len(parts):
                linkedin_id = parts[idx + 1]
    
    title = row.get('Title', '').strip() or None
    
    # Department - take first one
    dept_str = row.get('Departments', '').strip()
    department = dept_str.split(',')[0].strip() if dept_str else None
    
    # Phone
    phone = (row.get('Work phone number', '').strip() or 
            row.get('Mobile nUmber', '').strip() or 
            row.get('Company Phone Number', '').strip() or None)
    
    email = row.get('Email', '').strip() or None
    website = row.get('Website', '').strip() or None
    company_name = row.get('Company Name', '').strip() or None
    
    now = datetime.now().isoformat()
    
    return {
        'company_name': company_name,
        'name': full_name or None,
        'title': title,
        'linkedin_id': linkedin_id,
        'location': location,
        'department': department,
        'size': size,
        'company_id': None,
        'seniority_level': infer_seniority_level(title),
        'years_experience': None,
        'skills': '[]',
        'education': '[]',
        'profile_photo_url': None,
        'email_address': email,
        'phone_number': phone,
        'last_updated_at': now,
        'confidence_score': 75,
        'intent_signals': '[]',
        'company_funding_stage': None,
        'job_change_date': None,
        'technologies': '[]',
        'enrichment_status': '{}',
        'last_enriched_at': None,
        'work_email_enriched': email,
        'linkedin_url_enriched': linkedin_url or None,
        'social_profiles_enriched': '{}',
        'person_highlights_enriched': None,
        'company_domain_enriched': website,
        'user_id': None,
        'created_at': now,
        'updated_at': now,
        'popularity_index': 5.0,
    }


def prepare_csv(input_csv: str, output_csv: str) -> int:
    """Prepare CSV file with all table columns."""
    print(f"\n{'='*60}")
    print("STEP 1: Preparing CSV File")
    print(f"{'='*60}")
    
    row_count = 0
    skipped = 0
    
    with open(input_csv, 'r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        
        with open(output_csv, 'w', encoding='utf-8', newline='') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=INSERT_COLUMNS)
            writer.writeheader()
            
            for row in reader:
                processed = process_csv_row(row)
                
                # Skip rows without name (required for unique constraint)
                if not processed['name']:
                    skipped += 1
                    continue
                
                writer.writerow(processed)
                row_count += 1
                
                if row_count % 2000 == 0:
                    print(f"  Processed {row_count} rows...")
    
    print(f"✓ Prepared {row_count} rows (skipped {skipped})")
    return row_count


def insert_from_csv_fast(prepared_csv: str, db_config: dict, batch_size: int = 1000):
    """
    FAST INSERT using execute_values with optimizations.
    """
    print(f"\n{'='*60}")
    print("STEP 2: Fast Database Insert")
    print(f"{'='*60}")
    print(f"Batch Size: {batch_size}")
    
    columns_str = ', '.join(INSERT_COLUMNS)
    
    # SQL with ON CONFLICT for your UNIQUE(name, company_name, title)
    # Using COALESCE to handle NULLs in the unique constraint
    insert_sql = f"""
        INSERT INTO people_data ({columns_str})
        VALUES %s
        ON CONFLICT (name, company_name, title) DO UPDATE SET
            updated_at = EXCLUDED.updated_at,
            last_updated_at = EXCLUDED.last_updated_at
    """
    
    conn = None
    total_inserted = 0
    
    try:
        # Connect with optimized settings
        print(f"\nConnecting to {db_config['host']}...")
        conn = psycopg2.connect(
            **db_config,
            connect_timeout=30,
            options='-c statement_timeout=300000'  # 5 min timeout per statement
        )
        
        # CRITICAL: Set these for faster bulk inserts
        conn.autocommit = False
        cursor = conn.cursor()
        
        # Disable triggers temporarily for faster insert (optional, uncomment if needed)
        # cursor.execute("SET session_replication_role = 'replica';")
        
        # Count rows
        with open(prepared_csv, 'r', encoding='utf-8') as f:
            total_rows = sum(1 for _ in f) - 1
        
        print(f"Total rows to insert: {total_rows}")
        
        start_time = time.time()
        batch = []
        batch_num = 0
        
        with open(prepared_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                # Convert to tuple
                values = []
                for col in INSERT_COLUMNS:
                    val = row.get(col, '')
                    if val == '' or val == 'None':
                        val = None
                    elif col in ('years_experience', 'confidence_score'):
                        val = int(val) if val else None
                    elif col == 'popularity_index':
                        val = 5.0
                    values.append(val)
                
                batch.append(tuple(values))
                
                # Insert batch
                if len(batch) >= batch_size:
                    batch_num += 1
                    batch_start = time.time()
                    
                    execute_values(
                        cursor, 
                        insert_sql, 
                        batch,
                        page_size=batch_size  # Important for performance
                    )
                    conn.commit()
                    
                    total_inserted += len(batch)
                    batch_time = time.time() - batch_start
                    elapsed = time.time() - start_time
                    rate = total_inserted / elapsed
                    eta = (total_rows - total_inserted) / rate if rate > 0 else 0
                    
                    print(f"  Batch {batch_num}: {len(batch)} rows in {batch_time:.1f}s | "
                          f"{total_inserted}/{total_rows} ({100*total_inserted/total_rows:.0f}%) | "
                          f"{rate:.0f}/sec | ETA: {eta:.0f}s")
                    
                    batch = []
            
            # Final batch
            if batch:
                execute_values(cursor, insert_sql, batch, page_size=len(batch))
                conn.commit()
                total_inserted += len(batch)
        
        total_time = time.time() - start_time
        print(f"\n{'='*60}")
        print(f"✓ COMPLETED!")
        print(f"  Rows inserted: {total_inserted}")
        print(f"  Total time: {total_time:.1f}s")
        print(f"  Average rate: {total_inserted/total_time:.0f} rows/sec")
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def insert_copy_method(prepared_csv: str, db_config: dict):
    """
    FASTEST METHOD: Use PostgreSQL COPY command.
    This bypasses most PostgreSQL overhead and is 5-10x faster.
    NOTE: This doesn't handle ON CONFLICT - use for fresh tables only.
    """
    print(f"\n{'='*60}")
    print("STEP 2: SUPER FAST Insert using COPY")
    print(f"{'='*60}")
    
    conn = None
    
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Count rows first
        with open(prepared_csv, 'r', encoding='utf-8') as f:
            total_rows = sum(1 for _ in f) - 1
        
        print(f"Total rows: {total_rows}")
        print("Using COPY command (fastest method)...")
        
        start_time = time.time()
        
        with open(prepared_csv, 'r', encoding='utf-8') as f:
            # Skip header
            next(f)
            
            cursor.copy_expert(
                f"""
                COPY people_data ({', '.join(INSERT_COLUMNS)})
                FROM STDIN WITH (FORMAT CSV, NULL '', QUOTE '"')
                """,
                f
            )
        
        conn.commit()
        
        total_time = time.time() - start_time
        print(f"\n✓ COMPLETED in {total_time:.1f}s!")
        print(f"  Rate: {total_rows/total_time:.0f} rows/sec")
        
    except Exception as e:
        print(f"✗ COPY failed: {e}")
        print("Falling back to batch insert method...")
        if conn:
            conn.rollback()
            conn.close()
        insert_from_csv_fast(prepared_csv, db_config, 1000)
    finally:
        if conn:
            conn.close()


def main():
    # parser = argparse.ArgumentParser(description='Fast people data insert')
    # parser.add_argument('--input-csv', default='verified_data/next_set_of_data_700.csv')
    # parser.add_argument('--output-csv', default='verified_output/next_set_of_data_7k.csv')
    # parser.add_argument('--prepare-only', action='store_true')
    # parser.add_argument('--insert-only', action='store_true')
    # parser.add_argument('--batch-size', type=int, default=1000)
    # parser.add_argument('--use-copy', action='store_true', help='Use COPY command (fastest, but no ON CONFLICT)')
    # parser.add_argument('--no-confirm', action='store_true')
    
    # args = parser.parse_args()
    
    print("="*60)
    print("FAST People Data Insert Script")
    print("="*60)
    
    db_config = get_db_config()
    # print(f"\nInput:    {args.input_csv}")
    # print(f"Output:   {args.output_csv}")
    # print(f"Database: {db_config['host']}:{db_config['port']}/{db_config['database']}")
    # print(f"Method:   {'COPY (fastest)' if args.use_copy else f'Batch INSERT ({args.batch_size}/batch)'}")
    
    # if not args.no_confirm and not args.prepare_only:
    #     response = input("\nProceed? (yes/no): ").strip().lower()
    #     if response not in ('yes', 'y'):
    #         print("Cancelled.")
    #         return
    
    # # Step 1: Prepare CSV (if needed)
    # if not args.insert_only:
    #     prepare_csv(args.input_csv, args.output_csv)
    
    # # Step 2: Insert (if needed)
    # if not args.prepare_only:
    #     if args.use_copy:
    #         insert_copy_method(args.output_csv, db_config)
    #     else:
    #         insert_from_csv_fast(args.output_csv, db_config, args.batch_size)
    batch_size = 1000
    output_csvs = ['people/people_data_prepared_6k.csv', 'people/next_set_of_data_7k.csv', 'people/first_set_of_data_6k.csv']
    
    for i in output_csvs:
        print(f"Output:   {i}")
        print(f"Database: {db_config['host']}:{db_config['port']}/{db_config['database']}")
        print(f"Method:   {f'Batch INSERT ({batch_size}/batch)'}")
        insert_from_csv_fast(i, db_config, batch_size)

    print("\nDONE!")


if __name__ == '__main__':
    main()