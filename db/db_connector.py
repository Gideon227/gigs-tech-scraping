
import psycopg2
import pandas as pd
import os
import json
import ast  # For safely evaluating strings containing Python literals
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()
DATABASE = os.getenv("DATABASE")
USER = os.getenv("USER")
HOST = os.getenv("HOST")
PASSWORD = os.getenv("PASSWORD")
DATABASE_URL = os.getenv("DATABASE_URL")

def clean_array_string(array_str):
    """Convert string representation of array to actual list"""
    try:
        if isinstance(array_str, str):
            return ast.literal_eval(array_str)
        return array_str or []
    except (ValueError, SyntaxError):
        return []

def load_json_to_db(json_file):
    """Loads job data from JSON into PostgreSQL database"""
    
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


        # Batch insert
        insert_query = """
            INSERT INTO job (
                "companyName","companyLogo",
                "jobId", title, location, salary, 
                description, "roleCategory", responsibilities, skills,   
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
        
        conn.rollback()
    finally:
        
        cursor.close()
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
        cursor.execute("DELETE FROM job WHERE id = %s;", (job_id,))
        conn.commit()
        print(f"Job with id {job_id} deleted.")
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        cursor.close()
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


# if __name__ == "__main__":

#     remove_duplicate_jobids(host=HOST, database=DATABASE, user=USER, password=PASSWORD)
#     add_unique_constraint_on_jobid(host=HOST, database=DATABASE, user=USER, password=PASSWORD)
#      update_salary_from_json(
#         json_file=r"C:\Users\PC-022\Desktop\scrap\llm-job-scrap\update.json", 
#         host=HOST,
#         database=DATABASE,
#         user=USER,
#         password=PASSWORD
#     )
    # delete_job_by_id(
    #     "e3527a46-d26d-4909-adf6-f9d7ba882422",
    #     host=HOST,
    #     database=DATABASE,
    #     user=USER,
    #     password=PASSWORD
    # )
    # find_special_salary_rows()
    # find_salary_rows()
    
    # load_json_to_db(
    #     # json_file=r"C:\Users\David\OneDrive\Desktop\web scrap\microsoft\csvjson (2).json",  # Replace with your JSON file path
    #     # json_file=r"C:\Users\David\OneDrive\Desktop\web scrap\llm-job-scrap\Accenture1.json",
    #     # json_file=r"C:\Users\David\OneDrive\Desktop\web scrap\llm-job-scrap\Apex Systems.json",
    #     # json_file=r"C:\Users\David\OneDrive\Desktop\web scrap\llm-job-scrap\Deloitte (Microsoft Practice).json",
    #     # json_file=r"C:\Users\David\OneDrive\Desktop\web scrap\llm-job-scrap\DXC Technology.json",
    #     # json_file=r"C:\Users\David\OneDrive\Desktop\web scrap\llm-job-scrap\Nigel Frank International.json",
    #     # json_file=r"C:\Users\David\OneDrive\Desktop\web scrap\llm-job-scrap\Next Ventures.json",
    #     # json_file=r"C:\Users\David\OneDrive\Desktop\web scrap\llm-job-scrap\Pearson Carter.json",
    #     # json_file=r"C:\Users\David\OneDrive\Desktop\web scrap\llm-job-scrap\Prodapta (Ellis Group).json",
    #     # json_file=r"C:\Users\David\OneDrive\Desktop\web scrap\llm-job-scrap\MCA Connect.json",
    #     # json_file=r"C:\Users\David\OneDrive\Desktop\web scrap\llm-job-scrap\Digitall Nature Bulgaria EOOD.json",
    #     # json_file=r"C:\Users\David\OneDrive\Desktop\web scrap\llm-job-scrap\Tietoevry.json",
    #     # json_file=r"C:\Users\David\OneDrive\Desktop\web scrap\llm-job-scrap\Wipro.json",
    #     json_file=r"C:\Users\David\OneDrive\Desktop\web scrap\llm-job-scrap\treadted_grand.json",
        
    #     host=HOST,
    #     database=DATABASE,
    #     user=USER,
    #     password=PASSWORD
    # )
    
    
    
    
    
    
