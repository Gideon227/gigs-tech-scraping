from utils.temp_store import load_failed
from core.extractor import extract_fields
from db.db_connector import save_to_db

# def retry_failed():
#     for item in load_failed():
#         html = item.get("html")
#         url = item.get("url")
#         data = extract_fields(html, url)
#         if data:
#             save_to_db(data)