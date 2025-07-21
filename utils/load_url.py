import csv
# --- Load URLs from CSV ---
def load_urls_from_csv(filepath: str, column_name: str = "url", column_css:str='css:div'):
    urls = []
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = row.get(column_name)
            css=row.get(column_css)
            company_name=row.get("company_name")
            data = {
                "url": url,
                "wait_for":css,
                "company_name":company_name
            }
            if url:
                urls.append(data)
    return urls
