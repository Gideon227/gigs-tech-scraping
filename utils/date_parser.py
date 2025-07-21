# utils/date_parser.py
import dateparser
from datetime import datetime

def parse_posted_date(raw_date: str) -> str:
    """
    Converts formats like 'Today', '2 days ago', 'June 22, 2025' to 'YYYY-MM-DD'
    """
    if not raw_date:
        return ""

    parsed = dateparser.parse(raw_date, settings={"RELATIVE_BASE": datetime.now()})
    
    if not parsed:
        return ""
    # print("parsed date...", parsed)
    # return parsed.strftime("%Y-%m-%d")
    return parsed.strftime("%Y-%m-%d %H:%M:%S")
