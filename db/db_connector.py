
import psycopg2
import pandas as pd
import numpy as np
import os
import json
import ast  # For safely evaluating strings containing Python literals
from dotenv import load_dotenv
from psycopg2.extras import execute_values
# from utils import setup_scraping_logger
import logging


load_dotenv()
# DATABASE = os.getenv("DATABASE")
# USER = os.getenv("USER")
# HOST = os.getenv("HOST")
# PASSWORD = os.getenv("PASSWORD")
# DATABASE_URL = os.getenv("DATABASE_URL")



DATABASE = os.getenv("DB_NAME")
USER = os.getenv("PG_USER")
HOST = os.getenv("PG_HOST")
PASSWORD = os.getenv("PG_PASSWORD")
DATABASE_URL = os.getenv("DATABASE_URL")
# Configure logging
# logger = setup_scraping_logger("db_connector")
logger = logging.getLogger("db_connector")
logger.setLevel(logging.INFO)


def clean_array_string(value):
    """Safer array cleaning that handles more edge cases"""
    # Handle None and NaN for scalars
    if value is None:
        return []
   
    # Handle numpy arrays and pandas Series
    if isinstance(value, (float, int)) and pd.isna(value):
        return []
    if isinstance(value, (np.ndarray, pd.Series)):
        # Remove NaN and None
        return [str(item) for item in value if item is not None and not pd.isna(item)]
    # Handle lists
    if isinstance(value, list):
        return [str(item) for item in value if item is not None and not pd.isna(item)]
    # Handle strings
    if isinstance(value, str):
        try:
            # Handle string representations of lists
            if value.startswith('[') and value.endswith(']'):
                parsed = json.loads(value.replace("'", '"'))
                return [str(item) for item in parsed if item is not None]
            # Handle comma-separated strings
            return [item.strip() for item in value.split(",") if item.strip()]
        except:
            return [str(value)]
    return [str(value)]

def load_json_to_db_pt(json_file):
    """Loads job data from JSON into PostgreSQL database"""
    conn=cursor=None
    print("Password", PASSWORD)
    print("Database", DATABASE)
    print("User", USER)
    # prin("db_name",)
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=HOST,
            database=DATABASE,
            user=USER,
            password=PASSWORD
        )
        cursor = conn.cursor()

        cursor.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
#
        cursor.execute("""  
        CREATE TABLE IF NOT EXISTS job (  
            "id" UUID DEFAULT gen_random_uuid() PRIMARY KEY,  
            "jobId" TEXT DEFAULT '',  
            "title" TEXT DEFAULT '',  
            "description" TEXT DEFAULT '',  
            "location" TEXT DEFAULT '',  
            "country" TEXT DEFAULT '',  
            "state" TEXT DEFAULT '',  
            "city" TEXT DEFAULT '',  
            "jobType" TEXT DEFAULT 'fullTime',  
            "salary" TEXT DEFAULT '',  
            "skills" TEXT[] DEFAULT '{}',  
            "experienceLevel" TEXT DEFAULT 'experienced',  
            "currency" TEXT DEFAULT '',
            "applicationUrl" TEXT DEFAULT '',  
            "benefits" TEXT[] DEFAULT '{}',  
            "approvalStatus" TEXT,  
            "brokenLink" BOOLEAN DEFAULT FALSE,  
            "jobStatus" TEXT DEFAULT 'active', 
            "responsibilities" TEXT[] DEFAULT '{}',  
            "workSettings" TEXT,  
            "roleCategory" TEXT DEFAULT '',  
            "qualifications" TEXT[] DEFAULT '{}',  
            "companyLogo" TEXT DEFAULT '',  
            "companyName" TEXT ,
            "ipBlocked" BOOLEAN DEFAULT FALSE,  
            "createdAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  
            "updatedAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            "minSalary" INTEGER DEFAULT 0,
            "maxSalary" INTEGER DEFAULT 0,
            "postedDate" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            "category" TEXT
        )  
        """)

        
        if isinstance(json_file, str):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except UnicodeDecodeError:
                # Try with different encoding if UTF-8 fails
                with open(json_file, 'r', encoding='utf-8-sig') as f:
                    data = json.load(f)
            df = pd.DataFrame(data)
            print("len of job",len(df))
        else:
            df = pd.DataFrame(json_file)
            print("len of job direct access",len(df))
            
            # Drop rows where title is null or empty
        df = df[df['title'].notna() & (df['title'].astype(str).str.strip() != '')]
        print("len after dropping empty titles:", len(df))
        # Prepare rows for insertion
        rows = []
        for _, row in df.iterrows():
            
            max_salary = row.get('maxSalary', 0)
            min_salary = row.get('minSalary', 0)
            max_salary = 0 if pd.isna(max_salary) else int(max_salary)
            min_salary = 0 if pd.isna(min_salary) else int(min_salary)
            rows.append((
                row.get('companyName', ''),  # Default to 'Nigel Frank' if not provided
                row.get('companyLogo', ''),
                row.get('jobId', ''),  
                row.get('title', ''), 
                row.get('location', ''), 
                row.get('salary', ''),
                row.get('description', ''), 
                row.get("roleCategory",""),
                row.get("jobType"), 
                clean_array_string(row.get('responsibilities')),  # Convert to proper array
                clean_array_string(row.get('skills')),           # Convert to proper array
                row.get('applicationUrl', ''), 
                row.get('country', ''), 
                row.get('state', ''), 
                row.get('city', ''),
                row.get('currency', ''),
                max_salary,
                min_salary,
                clean_array_string(row.get('qualifications')),# Ensure integer type
                row.get("experienceLevel",""),
                clean_array_string(row.get('benefits', [])),  # Convert to proper array
                row.get("workSettings",""),
                row.get("postedDate"),
                row.get("category","")
                
                
            ))


        insert_query = """
            INSERT INTO job (
                "companyName","companyLogo",
                "jobId", title, location, salary, 
                description, "roleCategory", "jobType", responsibilities, skills,   
                "applicationUrl", country, state, city, currency, "minSalary", "maxSalary", qualifications,"experienceLevel", benefits,  "workSettings", "postedDate",category
            ) VALUES %s
            ON CONFLICT ("jobId") DO NOTHING
        """
        execute_values(cursor, insert_query, rows)

        conn.commit()
        print(f"Successfully inserted {len(rows)} records")
        return f"Successfully inserted {len(rows)} records"
    except Exception as e:
        print(f"Error: {e}")
        if conn:
            conn.rollback()
    finally:
        
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def delete_job_by_id(job_id, host, database, user, password):
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        cursor = conn.cursor()
        # Corrected: use double quotes for column name, not single quotes
        cursor.execute('DELETE FROM job WHERE "jobId" = %s;', (job_id,))
        conn.commit()
        print(f"Job with jobId {job_id} deleted.")
    except Exception as e:
        print("Error:", e)
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def find_salary_rows():
    try:
        conn = psycopg2.connect(
            host=HOST,
            database=DATABASE,
            user=USER,
            password=PASSWORD
        )
        cursor = conn.cursor()
       
        query = """
        SELECT id, salary FROM job
        """
        cursor.execute(query)
        conn.commit()
        # print(f"Updated {len(updates)} rows.")
        results = cursor.fetchall()
        print(f"Found {len(results)} rows:")
        for row in results:
            print(f"ID: {row[0]}, Salary: {row[1]}")
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        
        cursor.close()
        conn.close()

def find_special_salary_rows():
    try:
        conn = psycopg2.connect(
            host=HOST,
            database=DATABASE,
            user=USER,
            password=PASSWORD
        )
        cursor = conn.cursor()
        # Example: update salary for multiple ids with new uniform values
        updates = [
            
            ('£150000 - £160000 per year', '4452c598-c272-4528-a355-c2c9b37ef957'),
            ('£150000 - £180000 per year', '30c2d3fb-1544-4c26-b7eb-1effe5cce2e0'),
           
            

        ]
        
        for salary, job_id in updates:
            cursor.execute(
                "UPDATE job SET salary = %s WHERE id = %s;",
                (salary, job_id)
            )
        conn.commit()
        print(f"Updated {len(updates)} rows.")
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def update_salary_from_json(json_file, host, database, user, password):
    # Load JSON file into DataFrame
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    df = pd.DataFrame(data)

    # Only keep rows with both unique_id and tr_salary
    df = df[["unique_id", "tr_salary"]].dropna(subset=["unique_id", "tr_salary"])

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password
    )
    cursor = conn.cursor()

    updated = 0
    for _, row in df.iterrows():
        unique_id = row["unique_id"]
        tr_salary = row["tr_salary"]
        cursor.execute(
            "UPDATE job SET salary = %s WHERE \"jobId\" = %s;",
            (tr_salary, unique_id)
        )
        if cursor.rowcount > 0:
            updated += 1

    conn.commit()
    print(f"Updated salary for {updated} jobs.")
    cursor.close()
    conn.close()


def fetch_jobs_from_db(host, database, user, password):
    """Fetches all job records from the PostgreSQL database and prints them."""
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        cursor = conn.cursor()
        
        cursor.execute('SELECT "createdAt", "updatedAt" FROM job WHERE "jobId" = %s', ('nigel_frank_international_a0MaA000000fipt.2_1754359649',))
        # cursor.execute('SELECT "updatedAt" FROM job WHERE "jobId" LIKE %s', ('nigel_frank_international%',))
        result = cursor.fetchall()
        print("result", result)
        print()
        print()
        print("Created at:", result[0][0] if result else "No record found")
        print("Updated at:", result[0][1] if result else "No record found")
        # print("Updated at:", result[0] if result else "No record found")
        # cursor.execute("SELECT COUNT(*) FROM job")
        # count = cursor.fetchone()[0]
        # print("Total records:", count)
        # print("count",count)
      
       
    
        conn.commit()
        # Clean up
        cursor.close()
        conn.close()

    except Exception as e:
        print("Error fetching data from database:", e)



# import psycopg2

def remove_duplicate_jobids(host, database, user, password):
    """
    Delete duplicate rows in the job table based on jobId, keeping only one (the one with the lowest id).
    """
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        cursor = conn.cursor()
        # Delete duplicates, keep the row with the smallest UUID (or you can use MIN(createdAt) if you prefer)
        delete_query = """
        DELETE FROM job a
        USING job b
        WHERE
            a."jobId" = b."jobId"
            AND a.id > b.id;
        """
        cursor.execute(delete_query)
        conn.commit()
        print("Duplicate jobId rows removed, only one kept for each jobId.")
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def add_unique_constraint_on_jobid(host, database, user, password):
    """
    Add a unique constraint to the jobId column in the job table.
    """
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        cursor = conn.cursor()
        # Add unique constraint (if not already exists)
        alter_query = """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conname = 'job_jobid_key'
            ) THEN
                ALTER TABLE job ADD CONSTRAINT job_jobid_key UNIQUE ("jobId");
            END IF;
        END
        $$;
        """
        cursor.execute(alter_query)
        conn.commit()
        print("Unique constraint added to jobId column.")
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


# def parse_posted_date(date_str):
#     """Parse various date formats into datetime"""
#     if pd.isna(date_str) or not date_str:
#         return datetime.now()
#     try:
#         return datetime.strptime(date_str, "%Y-%m-%d")
#     except ValueError:
#         try:
#             return datetime.strptime(date_str, "%m/%d/%Y")
#         except ValueError:
#             return datetime.now()

def load_json_to_db(json_file, db_params):
    """Loads job data from JSON into PostgreSQL database
    
    Args:
        json_file: Path to JSON file or JSON data directly
        db_params: Dictionary containing database connection parameters
                  (host, database, user, password)
    
    Returns:
        str: Status message about the operation
    """
    conn = cursor = None
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=db_params['host'],
            database=db_params['database'],
            user=db_params['user'],
            password=db_params['password']
        )
        cursor = conn.cursor()

        # Create extension and table if not exists
        cursor.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
        cursor.execute("""  
        CREATE TABLE IF NOT EXISTS job (  
            "id" UUID DEFAULT gen_random_uuid() PRIMARY KEY,  
            "jobId" TEXT DEFAULT '',  
            "title" TEXT DEFAULT '',  
            "description" TEXT DEFAULT '',  
            "location" TEXT DEFAULT '',  
            "country" TEXT DEFAULT '',  
            "state" TEXT DEFAULT '',  
            "city" TEXT DEFAULT '',  
            "jobType" TEXT DEFAULT 'fullTime',  
            "salary" TEXT DEFAULT '',  
            "skills" TEXT[] DEFAULT '{}',  
            "experienceLevel" TEXT DEFAULT 'experienced',  
            "currency" TEXT DEFAULT '',
            "applicationUrl" TEXT DEFAULT '',  
            "benefits" TEXT[] DEFAULT '{}',  
            "approvalStatus" TEXT,  
            "brokenLink" BOOLEAN DEFAULT FALSE,  
            "jobStatus" TEXT DEFAULT 'active', 
            "responsibilities" TEXT[] DEFAULT '{}',  
            "workSettings" TEXT,  
            "roleCategory" TEXT DEFAULT '',  
            "qualifications" TEXT[] DEFAULT '{}',  
            "companyLogo" TEXT DEFAULT '',  
            "companyName" TEXT,
            "ipBlocked" BOOLEAN DEFAULT FALSE,  
            "createdAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  
            "updatedAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            "minSalary" INTEGER DEFAULT 0,
            "maxSalary" INTEGER DEFAULT 0,
            "postedDate" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            "category" TEXT
        )  
        """)

        # Load data
        if isinstance(json_file, str):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except UnicodeDecodeError:
                with open(json_file, 'r', encoding='utf-8-sig') as f:
                    data = json.load(f)
            df = pd.DataFrame(data)
            logger.info(f"Loaded {len(df)} jobs from file")
        else:
            df = pd.DataFrame(json_file)
            logger.info(f"Loaded {len(df)} jobs from direct input")
        
        # Data cleaning
        initial_count = len(df)
        df = df[df['title'].notna() & (df['title'].astype(str).str.strip() != '')]
        df = df.drop_duplicates(subset='jobId', keep='last')
        logger.info(f"Filtered {initial_count - len(df)} jobs with empty titles and duplicates")
        
        if len(df) == 0:
            logger.warning("No valid jobs to insert after filtering")
            return "No valid jobs to insert after filtering"

        # Prepare rows for insertion
        rows = []
        for _, row in df.iterrows():
            try:
                max_salary = row.get('maxSalary', 0)
                min_salary = row.get('minSalary', 0)
                max_salary = 0 if pd.isna(max_salary) else int(max_salary)
                min_salary = 0 if pd.isna(min_salary) else int(min_salary)
                
                # posted_date = parse_posted_date(row.get('postedDate'))
                
                rows.append((
                    row.get('companyName', ''),
                    row.get('companyLogo', ''),
                    row.get('jobId', ''),  
                    row.get('title', ''), 
                    row.get('location', ''), 
                    row.get('salary', ''),
                    row.get('description', ''), 
                    row.get('roleCategory', ''),
                    row.get('jobType'), 
                    clean_array_string(row.get('responsibilities')),
                    clean_array_string(row.get('skills')),
                    row.get('applicationUrl', ''), 
                    row.get('country', ''), 
                    row.get('state', ''), 
                    row.get('city', ''),
                    row.get('currency', ''),
                    max_salary,
                    min_salary,
                    clean_array_string(row.get('qualifications')),
                    row.get('experienceLevel', ''),
                    clean_array_string(row.get('benefits', [])),
                    row.get('workSettings', ''),
                    # posted_date,
                    row.get('postedDate'),
                    row.get('category', '')
                ))
            except Exception as e:
                logger.error(f"Error processing row: {e}")
                continue

        if not rows:
            logger.warning("No valid rows to insert after processing")
            return "No valid rows to insert after processing"

        # Insert data
        insert_query = """
            INSERT INTO job (
                "companyName", "companyLogo",
                "jobId", title, location, salary, 
                description, "roleCategory", "jobType", responsibilities, skills,   
                "applicationUrl", country, state, city, currency, 
                "minSalary", "maxSalary", qualifications, "experienceLevel", 
                benefits, "workSettings", "postedDate", category
            ) VALUES %s
            ON CONFLICT ("jobId") DO UPDATE
            SET title = EXCLUDED.title, "updatedAt" = NOW()

        """
        # ON CONFLICT ("jobId") DO NOTHING
        execute_values(cursor, insert_query, rows)
        conn.commit()
        
        success_msg = f"Successfully inserted {len(rows)} records"
        logger.info(success_msg)
        print(success_msg)
        return success_msg
        
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        if conn:
            conn.rollback()
        return f"Database error: {e}"
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        if conn:
            conn.rollback()
        return f"Unexpected error: {e}"
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    # Example usage

    # json_file = r"C:\Users\David\OneDrive\Desktop\web scrap\llm-job-scrap\grand_jobs_list.json"

    
    db_params = {
    'host': HOST,
    'database': DATABASE,
    'user': USER,
    'password': PASSWORD
}
    # nigel_frank_international_a0MP9000009GMQn.1_1753952304
    # load_json_to_db(json_file, db_params)
    # # load_json_to_db_pt(json_file)
    # delete_job_by_id("JOB-003", HOST, DATABASE, USER, PASSWORD)
    # fetch_jobs_from_db(host=HOST, database=DATABASE, user=USER, password=PASSWORD)

    # find_salary_rows()
    # find_special_salary_rows()
    # update_salary_from_json("path_to_your_json_file.json", HOST, DATABASE, USER, PASSWORD)
    # remove_duplicate_jobids(HOST, DATABASE, USER, PASSWORD)
    # add_unique_constraint_on_jobid(HOST, DATABASE, USER, PASSWORD)    
    
    
    
    
    
