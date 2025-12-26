import os
import csv
import logging
from typing import Dict, List, Tuple
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.pool import ThreadedConnectionPool

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
USER_ID = "632d6b76-0d5a-4074-b7e8-552b2a2aeb3d"
FIXED_POPULARITY_INDEX = 5.0

# Database configuration - Update these with your actual credentials
DB_CONFIG = {
    "database": "postgres",
    "user": "postgres",
    "password": "",
    "host": "localhost",
    "port": 5432,
    "minconn": 2,
    "maxconn": 10
}


class DatabaseManager:
    """Database manager with connection pooling"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.pool = None
    
    def __enter__(self):
        self.pool = ThreadedConnectionPool(
            minconn=self.config["minconn"],
            maxconn=self.config["maxconn"],
            database=self.config["database"],
            user=self.config["user"],
            password=self.config["password"],
            host=self.config["host"],
            port=self.config["port"]
        )
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.pool:
            self.pool.closeall()
    
    def company_exists(self, company_name: str) -> bool:
        """Check if a company already exists for the user"""
        query = """
        SELECT EXISTS(
            SELECT 1 FROM companies 
            WHERE user_id = %s AND LOWER(name) = LOWER(%s)
        )
        """
        
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, (USER_ID, company_name))
                result = cursor.fetchone()
                return result[0] if result else False
        finally:
            self.pool.putconn(conn)
    
    def insert_companies_batch(self, companies: List[Dict], batch_size: int = 100) -> int:
        """Insert companies in batches"""
        if not companies:
            return 0
        
        now = datetime.now(timezone.utc)
        total_inserted = 0
        
        for i in range(0, len(companies), batch_size):
            batch = companies[i:i + batch_size]
            values = []
            
            for company in batch:
                values.append((
                    USER_ID,  # user_id
                    company.get('name', ''),
                    company.get('domain', ''),
                    company.get('linkedin_url', ''),
                    company.get('headquarters', ''),
                    company.get('location', ''),
                    company.get('description', ''),
                    company.get('primary_industry', ''),
                    company.get('size', ''),
                    company.get('revenue_range', ''),
                    company.get('revenue_currency', 'USD'),
                    company.get('country', ''),
                    company.get('company_type', ''),
                    company.get('employee_count'),
                    now,  # created_at
                    now,  # updated_at
                    company.get('write_idempotency_key', ''),
                    None,  # company_snapshot
                    None,  # positive_news
                    None,  # core_values
                    None,  # gtm_strategy
                    None,  # revenue_model
                    'pending',  # enrichment_status
                    None,  # key_people
                    None,  # financials
                    None,  # recent_news
                    None,  # tech_stack
                    None,  # contact_info
                    None,  # social_media
                    None,  # social_media_data
                    None,  # latest_news_data
                    None,  # competitive_research_data
                    None,  # buyer_intent_data
                    None,  # events_data
                    FIXED_POPULARITY_INDEX  # popularity_index
                ))
            
            try:
                self._execute_batch_insert(values)
                total_inserted += len(batch)
                logger.info(f"Inserted batch of {len(batch)} companies (total: {total_inserted})")
            except Exception as e:
                logger.error(f"Error inserting batch: {e}")
        
        return total_inserted
    
    def _execute_batch_insert(self, values: List[Tuple]):
        """Execute batch insert"""
        query = """
        INSERT INTO companies (
            id, user_id, name, domain, linkedin_url, headquarters, location,
            description, primary_industry, size, revenue_range, revenue_currency,
            country, company_type, employee_count, created_at, updated_at,
            write_idempotency_key, company_snapshot, positive_news, core_values, 
            gtm_strategy, revenue_model, enrichment_status, key_people, financials, 
            recent_news, tech_stack, contact_info, social_media, social_media_data, 
            latest_news_data, competitive_research_data, buyer_intent_data, events_data, 
            popularity_index
        ) VALUES %s
        ON CONFLICT (user_id, name) DO NOTHING
        """
        
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cursor:
                execute_values(cursor, query, values)
                conn.commit()
                return len(values)
        finally:
            self.pool.putconn(conn)


def read_enriched_csv() -> List[Dict]:
    """Read the pre-enriched CSV file and return list of company dictionaries"""
    companies = []
    file_paths = ["output/prod_enriched_companies_300.csv", "output/prod_enriched_companies_800.csv"]
    try:
        for file_path in file_paths:
            with open(file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                
                for row in reader:
                    try:
                        # Parse employee_count - handle empty strings and convert to int
                        employee_count_str = row.get('employee_count', '').strip()
                        if employee_count_str:
                            try:
                                employee_count = int(float(employee_count_str))
                            except ValueError:
                                employee_count = None
                        else:
                            employee_count = None
                        
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
                            'revenue_currency': row.get('revenue_currency', 'USD').strip(),
                            'country': row.get('country', '').strip(),
                            'company_type': row.get('company_type', '').strip(),
                            'employee_count': employee_count,
                            'revenue_usd_m': row.get('revenue_usd_m', '').strip(),
                            'write_idempotency_key': row.get('write_idempotency_key', '').strip(),
                        }
                        companies.append(company)
                    except Exception as e:
                        logger.warning(f"Skipping row due to error: {e} - Row: {row}")
                        continue
                    
    except FileNotFoundError:
        logger.error(f"CSV file not found: {file_path}")
        raise
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        raise
    
    logger.info(f"Successfully read {len(companies)} companies from CSV")
    return companies


def process_companies(companies: List[Dict], db_manager: DatabaseManager) -> Tuple[List[Dict], List[Dict]]:
    """Check companies against database and return lists of new and existing companies"""
    
    companies_to_insert = []
    skipped_companies = []
    
    logger.info(f"Checking {len(companies)} companies against database...")
    
    for idx, company in enumerate(companies):
        if (idx + 1) % 100 == 0:
            logger.info(f"Checked {idx + 1}/{len(companies)} companies...")
        
        # Check if company exists for this user
        if db_manager.company_exists(company['name']):
            skipped_companies.append({
                'name': company['name'],
                'reason': 'Already exists in database',
                'country': company.get('country', '')
            })
        else:
            companies_to_insert.append(company)
    
    logger.info(f"Found {len(companies_to_insert)} new companies, skipping {len(skipped_companies)} existing")

    return companies_to_insert, skipped_companies


def main():
    """Main function to read CSV and insert into database"""
    start_time = datetime.now(timezone.utc)
    logger.info(f"Starting processing at {start_time}")

    # Initialize database manager with connection pooling
    with DatabaseManager(DB_CONFIG) as db_manager:
        # Read enriched CSV file
        companies = read_enriched_csv()

        # Check which companies already exist
        companies_to_insert, skipped_companies = process_companies(companies, db_manager)

        # Insert new companies into database in batches
        if companies_to_insert:
            logger.info(f"Inserting {len(companies_to_insert)} companies into database...")
            inserted_count = db_manager.insert_companies_batch(companies_to_insert, batch_size=100)
            logger.info(f"Successfully inserted {inserted_count} companies into database")
        else:
            logger.info("No new companies to insert")

        # Print summary
        logger.info("=" * 60)
        logger.info("PROCESSING SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total companies in CSV: {len(companies)}")
        logger.info(f"Companies inserted: {len(companies_to_insert)}")
        logger.info(f"Companies skipped (already exist): {len(skipped_companies)}")
        logger.info("=" * 60)

    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).total_seconds()
    logger.info(f"Processing completed in {duration:.2f} seconds")


if __name__ == "__main__":
    main()