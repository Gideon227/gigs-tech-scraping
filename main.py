import asyncio
import os
import json
import hashlib
from datetime import datetime, timedelta
from core.extractor import job_detail_extractor_from_url, keyword_first_job_list  # both come from extractor.py
from utils.temp_store import save_failed
from db.db_connector import load_json_to_db
from utils.load_url import load_urls_from_csv
from utils.logger import setup_scraping_logger
from utils.email_sender import send_completion_email

# Optional: WhatsApp via Twilio
# try:
#     from twilio.rest import Client as TwilioClient
# except Exception:
#     TwilioClient = None

logger = setup_scraping_logger(name="Job scraping")

# -------------------------------
# Helpers (unchanged + new)
# -------------------------------

def generate_unique_id(data, company_name):
    job_id_raw = (data.get("jobId") or "").strip()
    company_name = (data.get("companyName") or company_name).strip().replace(" ", "_").lower()
    if job_id_raw:
        return f"{company_name}_{job_id_raw}"
    application_url = data.get("applicationUrl")
    title = data.get("title", "")
    location = data.get("location", "")
    fallback_string = f"{company_name}_{title}_{location}_{application_url}"
    hash_digest = hashlib.sha256(fallback_string.encode("utf-8")).hexdigest()[:10]
    return f"{company_name}_{hash_digest}"

def append_jsonl(data, filename="bo_job_data_llm_single.jsonl"):
    with open(filename, "a", encoding="utf-8") as f:
        json.dump(data, f)
        f.write("\n")

def convert_date(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    return f"{obj}"

def save_to_json(data, filename="new_grand_jobs_list.json"):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        return filename

# -------------------------------
# Keywords
# -------------------------------

def _load_keywords():
    # Default to your provided list; can still be overridden via JOB_KEYWORDS env
    raw = os.getenv("JOB_KEYWORDS")
    if not raw:
        return [
            "power platform",
            "power",
            "dynamics 356",
            "dynamic",
            # "dynamics crm",
            # "dataverse",
            # "Power Apps",
            # "Power BI",
            # "Power Automate",
            # "canvas app",
            # "model-driven app",
            # "RPA"
        
            # Power Apps, Dataverse, Power BI, Power Automate, canvas app, model-driven app, RPA
        ]
    return [w.strip() for w in raw.split(",") if w.strip()]

# -------------------------------
# Date filtering: keep only <= 30 days old
# -------------------------------

def _parse_date(dt_str: str):
    if not dt_str:
        return None
    dt_str = str(dt_str).strip()
    # Try ISO first
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        pass
    # Try a few common formats
    fmts = [
        "%Y-%m-%d",
        "%b %d, %Y",
        "%B %d, %Y",
        "%d %b %Y",
        "%d %B %Y",
    ]
    for fmt in fmts:
        try:
            return datetime.strptime(dt_str, fmt)
        except Exception:
            continue
    return None

def _is_within_days(dt_str: str, days: int = 30) -> bool:
    dt = _parse_date(dt_str)
    if not dt:
        return True  # if unknown, keep (some sites omit dates)
    return (datetime.now() - dt) <= timedelta(days=days)

# -------------------------------
# Main run
# -------------------------------
async def run():
    print("start function ✈️✈️")
    logger.info("Starting web scraping job")

    open_provider = os.getenv("PROVIDER")
    openai_api_token = os.getenv("OPENAI_API_KEY")

    # urls = load_urls_from_csv("updated_treated_source.csv", column_name='url', column_css="wait_for")
    urls = load_urls_from_csv("job_scraper_template.csv", column_name='url', column_css="wait_for")
    # urls=urls[0]
    print('url', urls)
    print('url len', len(urls))
    logger.info(f"Found {len(urls)} sites")
    
    keywords = _load_keywords()
    logger.info(f"Using keywords: {keywords}")
    
    grand_jobs_list = []

    for i, site in enumerate(urls):
        print()
        print()
        print("===========")
        print("site", site)
        company_job_list = []
        company_name = site.get("company_name") or site.get("company") or ""
        try:
            logger.info(f"Processing {i+1}/{len(urls)}: {company_name}")

            for kw in keywords:
                try:
                    # Keyword-first collection of application URLs
                    seed_jobs = await keyword_first_job_list({**site, "company_name": company_name}, kw, open_provider, openai_api_token)
                except Exception as extractor_error:
                    logger.error(f"List extractor failed for {company_name} ({kw}): {str(extractor_error)}")
                    save_failed({
                        "company": company_name,
                        "url": site,
                        "error": str(extractor_error),
                        "type": "list_extractor_error",
                        "keyword": kw,
                    })
                    continue

                logger.info(f"{company_name}  '{kw}' → {len(seed_jobs)} jobs (pre-filter)")
                print(f"<<<<<< ✅ Detail scraping for {company_name}  '{kw}' ✅ >>>>>>")

                # Detail stage
                for m, job in enumerate(seed_jobs):
                    logger.info(f"Processing {m+1}/{len(seed_jobs)}: {company_name}")
                    application_url = job.get("applicationUrl")
                    if not application_url:
                        continue

                    # Optional optimization: skip detail fetch if list-date is older than 30d
                    list_date = job.get("postedDate")
                    # if list_date and not _is_within_days(list_date, 30):
                    #     logger.info(f"Skip (older than 30d by list date): {application_url}")
                    #     continue

                    data = await job_detail_extractor_from_url(
                        url=application_url,
                        provider=open_provider,
                        api_token=openai_api_token,
                    )

                    # After detail extraction, re-check date filter
                    site_postedDate = data.get("postedDate") or list_date
                    # if not _is_within_days(site_postedDate, 30):
                    #     logger.info(f"Skip (older than 30d by detail date): {application_url}")
                    #     continue

                    postedDate_now = convert_date(datetime.now())
                    jobId = generate_unique_id(data or {}, company_name)

                    list_data = {
                        "jobId": jobId,
                        "title": (data or {}).get("title", ""),
                        "applicationUrl": (data or {}).get("applicationUrl") or application_url,
                        "description": (data or {}).get("description"),
                        "location": (data or {}).get("location"),
                        "country": (data or {}).get("country"),
                        "state": (data or {}).get("state"),
                        "city": (data or {}).get("city"),
                        "jobType": (data or {}).get("jobType"),
                        "salary": (data or {}).get("salary"),
                        "skills": (data or {}).get("skills"),
                        "experienceLevel": (data or {}).get("experienceLevel"),
                        "currency": (data or {}).get("currency"),
                        "benefits": (data or {}).get("benefits"),
                        "approvalStatus": (data or {}).get("approvalStatus"),
                        "jobStatus": (data or {}).get("jobStatus"),
                        "responsibilities": (data or {}).get("responsibilities"),
                        "workSettings": (data or {}).get("workSettings"),
                        "roleCategory": (data or {}).get("roleCategory"),
                        "qualifications": (data or {}).get("qualifications"),
                        "companyLogo": (data or {}).get("companyLogo"),
                        "companyName": job.get("companyName", company_name),
                        "minSalary": (data or {}).get("minSalary"),
                        "maxSalary": (data or {}).get("maxSalary"),
                        "postedDate": site_postedDate or postedDate_now,
                        "category": (data or {}).get("category"),
                        "updatedAt": postedDate_now,
                    }

                    company_job_list.append(list_data)
                    grand_jobs_list.append(list_data)
        except Exception as e:
            save_failed({"url": site, "error": str(e)})

        logger.info(f"Save {len(company_job_list)} for {company_name}")
        print(f"Save {len(company_job_list)} for {company_name}")

    final_data_path = save_to_json(grand_jobs_list)
    print("saved to file:", final_data_path)
    return grand_jobs_list

if __name__ == "__main__":
    result = asyncio.run(run())

    DATABASE = os.getenv("DATABASE")
    USER = os.getenv("USER")
    HOST = os.getenv("HOST")
    PASSWORD = os.getenv("PASSWORD")

    db_params = {
        'host': HOST,
        'database': DATABASE,
        'user': USER,
        'password': PASSWORD
    }

    failed_jobs = []
    saved_count = 0
    if result:
        saved_count = load_json_to_db(result, db_params)  # upsert handled inside
        print("print saved ✅✅✈️✅")

    try:
        with open("failed_jobs.json", "r", encoding="utf-8") as f:
            failed_jobs = json.load(f)
    except Exception:
        pass

    success_count = len(result or [])
    failed_count = len(failed_jobs)
    email_body = (
        f"The job scraping script has finished running using the new pipline.\n\n"
        f"Successful jobs (post-filter): {success_count}\n"
        f"Saved jobs: {saved_count}\n"
        f"Failed jobs: {failed_count}\n\n"
        f"Failed jobs details:\n{json.dumps(failed_jobs, indent=2, ensure_ascii=False)}"
    )

    # Email summary
    send_completion_email(
        subject="Job Scraping Completed",
        body=email_body,
        to_email=os.getenv("MAIL_TO"),
    )

    # # WhatsApp summary via Twilio (optional)
    # try:
    #     if TwilioClient:
    #         sid = os.getenv("TWILIO_ACCOUNT_SID")
    #         token = os.getenv("TWILIO_AUTH_TOKEN")
    #         wa_from = os.getenv("TWILIO_WHATSAPP_FROM")  # e.g., 'whatsapp:+14155238886'
    #         wa_to = os.getenv("TWILIO_WHATSAPP_TO")      # e.g., 'whatsapp:+234XXXXXXXXXX'
    #         if sid and token and wa_from and wa_to:
    #             client = TwilioClient(sid, token)
    #             client.messages.create(from_=wa_from, to=wa_to, body=email_body[:1500])
    # except Exception as twerr:
    #     logger.error(f"Twilio WhatsApp send failed: {twerr}")

    print(f"Final job list {len(result or [])}")
