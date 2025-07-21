from datetime import datetime
KEYWORDS = ["power platform", "power automate", "power apps", "dynamics 365", "d365", "crm", "erp"]

def is_relevant_job(job: dict) -> bool:
    title = (job.get("title") or "").lower()
    desc = (job.get("description") or "").lower()
    if not any(k in title or k in desc for k in KEYWORDS):
        return False

    date_str = job.get("postedDate")
    if date_str:
        try:
            posted_date = datetime.strptime(date_str, "%Y-%m-%d")
            return (datetime.now() - posted_date).days <= 15
        except:
            return False
    return True