
import csv

def load_urls_from_csv(filepath, column_name="url", column_css="wait_for"):
    """
    Load rows from a CSV into a list of dicts ready for scraping.

    Args:
        filepath (str): Path to CSV file.
        column_name (str): The column that contains the base job site URL.
        column_css (str): The column that contains the selector to wait for.

    Returns:
        list[dict]: List of dicts, each representing a site config row.
    """
    rows = []
    with open(filepath, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Normalize keys: lower-case + strip spaces
            print("row", row)
            normalized = {k.strip(): v.strip() for k, v in row.items() if k}

            site = {
                "company_name": normalized.get("company_name") or "",
                "url": normalized.get(column_name) or "",
                "wait_for": normalized.get(column_css) or ".overview",
                "search_url_template": normalized.get("search_url_template") or "",
                "pagination_param": normalized.get("pagination_param") or "",
                "search_input_selector": normalized.get("search_input_selector") or "",
                "search_submit_selector": normalized.get("search_submit_selector") or "",
                "search_enter": normalized.get("search_enter") or "",
                "results_container_selector": normalized.get("results_container_selector") or "",
                "job_link_selector": normalized.get("job_link_selector") or "",
                "job_card_selector": normalized.get("job_card_selector") or "",
                "modal_selector": normalized.get("modal_selector") or "",
                "modal_apply_link_selector": normalized.get("modal_apply_link_selector") or "",
                "modal_close_selector": normalized.get("modal_close_selector") or "",
                "pagination_next_selector": normalized.get("pagination_next_selector") or "",
                "infinite_scroll": (normalized.get("infinite_scroll") or "false").lower() == "true",
                "max_pages": int(normalized.get("max_pages") or 50),
            }

            if site["url"]:  # skip empty rows
                rows.append(site)
    print("len of row", len(rows))
    return rows
