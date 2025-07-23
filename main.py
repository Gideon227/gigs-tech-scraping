import asyncio
from core.extractor import job_list_extractor, job_detail_extractor_from_url
from utils.temp_store import save_failed
from db.db_connector import load_json_to_db
# from utils.notifier import send_email, send_whatsapp_alert
# from core.retry_handler import retry_failed
import json
from utils.load_url import load_urls_from_csv
import os
from datetime import datetime
import hashlib
from utils.logger import setup_scraping_logger
from utils.email_sender import send_completion_email
logger = setup_scraping_logger(name="Job scrapinng")

def generate_unique_id(data):
    job_id_raw = (data.get("jobId") or "").strip()
    company_name = (data.get("companyName") or "").strip().replace(" ", "_").lower()

    if job_id_raw:
        return f"{company_name}_{job_id_raw}"

    # Fallback: use hash of application URL + title + location
    application_url = data.get("applicationUrl")
    title = data.get("title", "")
    location = data.get("location", "")

    fallback_string = f"{company_name}_{title}_{location}_{application_url}"
    hash_digest = hashlib.sha256(fallback_string.encode("utf-8")).hexdigest()[:10]  # short hash
    return f"{company_name}_{hash_digest}"

def append_jsonl(data, filename="bo_job_data_llm_single.jsonl"):
    with open(filename, "a", encoding="utf-8") as f:
        json.dump(data, f)
        f.write("\n")
        
def convert_date(obj):
        if isinstance(obj,datetime):
            return obj.isoformat()
        else: return f"{obj}"
        # raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

def save_to_json(data, filename="grand_jobs_list.json"):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        return filename
async def run():

    logger.info("Starting web scraping job")
    open_provider =os.getenv("PROVIDER")
    openai_api_token = os.getenv("OPENAI_API_KEY")
        
    urls = load_urls_from_csv(r"treat_static_job.csv",column_name='power_url',column_css="wait_for")
    logger.info(f"Found {len(urls)} sites")
    grand_jobs_list = []
     
    for i, url in enumerate(urls):
        company_job_list=[]
        try:
            company_name=url["company_name"]
            print(f"\n📄 Processing {i+1}/{len(urls)}: {company_name}")
            logger.info(f"Processing {i+1}/{len(urls)}: {company_name}")
            jobs_per_site = await job_list_extractor(url=url, provider=open_provider, api_token=openai_api_token)
            
            logger.info(f"Found {len(jobs_per_site)} of jobd")
            # append_jsonl(jobs_per_site, filename="sites_datas_store.jsonl")
            # print(f"Found number {len(jobs_per_site)} of job") 
            
            
            if jobs_per_site:
                # append_jsonl(jobs_per_site, filename=f"{company_name}_backup.jsonl")
               
                print(f"<<<<<< ✅Scraping detail page✅ >>>>>>")
                for m, job in enumerate(jobs_per_site):
                  
                    print(f"\n📄 Processing {m+1}/{len(jobs_per_site)}: {company_name}")
                    logger.info(f"Processing {m+1}/{len(jobs_per_site)}: {company_name}")
                    # print("job", job)
                    application_url = job.get("applicationUrl")
                    print("applicayion", application_url)
                    if not application_url:
                        print(f"⚠️ Missing application URL for job: {job.get('title')}")
                        continue

                    data = await job_detail_extractor_from_url(url=application_url, provider=open_provider, api_token=openai_api_token) #returns a dictionary
                    postedDate = datetime.now()
                    postedDate=convert_date(postedDate)
                    site_postedDate= job.get("postedDate") or data.get("postedDate")
                    # connstruct_jobid to track duplicates
                    jobId = generate_unique_id(data)
                    print("unique job id", jobId)
                    list_data = {
                        "jobId":jobId,
                        "title":data.get("title",""),
                        "applicationUrl":data.get("applicationUrl") or job.get("applicationUrl",""),
                        "description":data.get("description"),
                        "location":data.get("location"),
                        "country":data.get("country"),
                        "state":data.get("state"),
                        "city":data.get("city"),
                        "jobType":data.get("jobType"),
                        "salary":data.get("salary"),
                        "skills":data.get("skills"),
                        "experienceLevel":data.get("experienceLevel"),
                        "currency":data.get("currency"),
                        "benefits":data.get("benefits"),
                        "approvalStatus":data.get("approvalStatus"),
                        "jobStatus":data.get("jobStatus"),
                        "responsibilities":data.get("responsibilities"),
                        "workSettings":data.get("workSettings"),
                        "roleCategory":data.get("roleCategory"),
                        "qualifications":data.get("qualifications"),
                        "companyLogo":data.get("companyLogo"),
                        "companyName":data.get("companyName"),
                        "minSalary":data.get("minSalary"),
                        "maxSalary":data.get("maxSalary"),
                        "postedDate":site_postedDate or postedDate,
                        "category":data.get("category"),                         
                    }
                    # save for each site
                    company_job_list.append(list_data)
                    grand_jobs_list.append(list_data)
            else: 
                print("⚠️ No jobs found on page.")
                save_failed({"url": url, "html": data})
                    # send_email("Extraction Error", f"Failed to extract from {url}")
                    # send_whatsapp_alert(f"⚠️ Extraction failed: {url}")
            # save_failed({"url": url, "html": data})
            # send_email("Scraping Error", f"Failed to fetch {url}")
            # send_whatsapp_alert(f"⚠️ Fetch failed: {url}")
        except Exception as e:           
            # print(f"❌ Error while processing {url.get('company_name')}: {e}")
            save_failed({"url": url, "error": str(e)})
        logger.info(f"Save {len(company_job_list)} for {company_name}") 
        print(f"Save {len(company_job_list)} for {company_name}")   
        # save_to_json(company_job_list, filename=f"{company_name}.json")
    final_data_path = save_to_json(grand_jobs_list) 
    print("saved to file:", final_data_path)  
    return grand_jobs_list 
        
if __name__ == "__main__":
   
    result = asyncio.run(run())    
    
    if result:
        # save to database
        laod_data = load_json_to_db(result)
        print("print saved ✅✅✈️✅")
        failed_jobs = []
    try:
        with open("failed_jobs.json", "r", encoding="utf-8") as f:
                failed_jobs = json.load(f)
    except Exception:
        pass
    success_count = len(result)
    failed_count = len(failed_jobs)
    email_body = (
            f"The job scraping script has finished running.\n\n"
            f"Successful jobs: {success_count}\n"
            f"Saved jobs: {laod_data}\n"
            f"Failed jobs: {failed_count}\n\n"
            f"Failed jobs details:\n{json.dumps(failed_jobs, indent=2, ensure_ascii=False)}"
        )
    send_completion_email(
            subject="Job Scraping Completed",
            body=email_body,
            to_email=os.getenv("MAIL_TO"),
        ) 
    
    print(f"Final job list {len(result)}")

    
    # transform data