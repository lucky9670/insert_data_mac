import psycopg2
import time
import csv
from psycopg2.extras import execute_batch
from typing import Dict, List, Optional

# Database config - USE ENVIRONMENT VARIABLES IN PRODUCTION
DB_CONFIG = {
    "host": "db.rhgkggssbehwhocqsmwv.supabase.co",
    "port": "5432",
    "dbname": "postgres",
    "user": "postgres",
    "password": "Noida305901"
}


# CSV file path
CSV_FILE_PATH = "top_500_global_retail_companies.csv"

# Batch size for bulk updates
BATCH_SIZE = 100


def read_csv_data(file_path: str) -> List[Dict]:
    """
    Read company data from CSV file.
    Only extracts company names for matching.
    """
    companies = []
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                company = {
                    'name': row.get('Company Name', row.get('name', '')).strip(),
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


def get_company_id_by_name(cur, name: str) -> Optional[str]:
    """
    Check if company exists in DB by name (case-insensitive) and return its ID.
    Searches one company at a time.
    
    Args:
        cur: Database cursor
        name: Company name to search
        
    Returns:
        Company ID if found, None otherwise
    """
    query = "SELECT id FROM company_master WHERE LOWER(name) = LOWER(%s)"
    cur.execute(query, (name,))
    result = cur.fetchone()
    return result[0] if result else None


def bulk_update_primary_industry(cur, conn, company_ids: List[str], industry: str = "Retail"):
    """
    Bulk update primary_industry for given company IDs.
    
    Args:
        cur: Database cursor
        conn: Database connection
        company_ids: List of company IDs to update
        industry: Industry value to set (default: 'Retail')
        
    Returns:
        Number of records updated
    """
    if not company_ids:
        print("⚠️ No company IDs to update")
        return 0
    
    # Prepare data for execute_batch: list of tuples (industry, id)
    updates = [(industry, company_id) for company_id in company_ids]
    
    query = "UPDATE company_master SET primary_industry = %s WHERE id = %s"
    execute_batch(cur, query, updates, page_size=BATCH_SIZE)
    conn.commit()
    
    return len(updates)


def main():
    print("🚀 Starting Primary Industry Update to 'Retail'")
    print(f"⏰ Start time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📄 CSV file: {CSV_FILE_PATH}")
    print(f"🏭 Target Industry: Retail")
    print(f"📦 Batch Size: {BATCH_SIZE}")
    print("="*60)
    
    # Read companies from CSV
    companies = read_csv_data(CSV_FILE_PATH)
    
    if not companies:
        print("❌ No companies found in CSV")
        return
    
    # Connect to database
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    total_count = len(companies)
    found_ids = []  # List to collect company IDs found in DB
    not_found_names = []  # List of company names not found in DB
    
    print(f"\n{'='*60}")
    print(f"📊 Total companies in CSV: {total_count}")
    print(f"🔍 Searching for companies in database one by one...")
    print(f"{'='*60}\n")
    
    try:
        # Search for each company one by one
        for idx, company in enumerate(companies):
            progress = idx + 1
            pct = (progress / total_count) * 100
            company_name = company['name']
            
            # Search for company in database by name only (case-insensitive)
            company_id = get_company_id_by_name(cur, company_name)
            
            if company_id:
                found_ids.append(company_id)
                print(f"[{progress}/{total_count}] ({pct:.1f}%) ✅ FOUND: {company_name[:50]}")
            else:
                not_found_names.append(company_name)
                print(f"[{progress}/{total_count}] ({pct:.1f}%) ❌ NOT FOUND: {company_name[:50]}")
        
        print(f"\n{'='*60}")
        print(f"🔍 Search Complete!")
        print(f"   - Total in CSV: {total_count}")
        print(f"   - Found in DB: {len(found_ids)}")
        print(f"   - Not Found: {len(not_found_names)}")
        print(f"{'='*60}\n")
        
        # Bulk update primary_industry for all found companies
        if found_ids:
            print(f"📦 Performing bulk update for {len(found_ids)} companies...")
            print(f"   Setting primary_industry = 'Retail'")
            
            # Process in batches if list is very large
            total_updated = 0
            for i in range(0, len(found_ids), BATCH_SIZE):
                batch = found_ids[i:i + BATCH_SIZE]
                updated_count = bulk_update_primary_industry(cur, conn, batch, "Retail")
                total_updated += updated_count
                print(f"   ✅ Updated batch {i//BATCH_SIZE + 1}: {updated_count} records")
            
            print(f"\n{'='*60}")
            print(f"✅ Bulk Update Complete!")
            print(f"   - Total Updated: {total_updated}")
            print(f"{'='*60}")
        else:
            print("⚠️ No companies found in database to update")
        
        # Optionally log not found companies
        if not_found_names:
            print(f"\n📋 Companies NOT found in database ({len(not_found_names)}):")
            for name in not_found_names[:20]:  # Show first 20
                print(f"   - {name}")
            if len(not_found_names) > 20:
                print(f"   ... and {len(not_found_names) - 20} more")
        
    except KeyboardInterrupt:
        print("\n⚠️ Process interrupted by user")
        # Commit any pending updates
        if found_ids:
            print(f"💾 Saving {len(found_ids)} found companies before exit...")
            bulk_update_primary_industry(cur, conn, found_ids, "Retail")
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


if __name__ == "__main__":
    main()