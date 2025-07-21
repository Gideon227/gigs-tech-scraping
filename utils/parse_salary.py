import re

def parse_salary_new(salary_text):
    if not salary_text or not isinstance(salary_text, str):
        return ""

    # Normalize and clean input
    salary_text = (
        salary_text.lower()
        .replace(",", "")
        .replace("–", "-")
        .replace("—", "-")
        .replace(" to ", "-")
        .replace(" or ", " | ")
        .replace("/", " per ")
        .replace("usd", "$")
        .replace("us$", "$")
        .replace("aud", "$aud")
        .replace("gbp", "£")
        .replace("eur", "€")
        .strip()
    )

    # Split parts if multiple ranges present (keep only first meaningful one)
    parts = re.split(r"\s*\|\s*", salary_text)
    salary_text = parts[0]  # pick the first segment before "or", "/", etc.

    # Period detection
    period_map = {
        'hour': 'hour',
        'hr': 'hour',
        'day': 'day',
        'month': 'month',
        'mo': 'month',
        'year': 'year',
        'yr': 'year',
        'annum': 'year',
        'annually': 'year'
    }
    
    period = ""
    period_match = re.search(r"(hour|hr|day|month|mo|year|yr|annum|annually)", salary_text)
    if period_match:
        period = " per " + period_map.get(period_match.group(1), 'year')

    # Currency detection
    currency = ""
    currency_match = re.search(r"([$£€]|\$aud)", salary_text)
    if currency_match:
        currency = currency_match.group(1)

    # Check if input contains a range (hyphen)
    has_range = "-" in salary_text.replace(" ", "")

    # Salary range regex
    if has_range:
        range_regex = re.search(
            r"(?:[$£€]|\$aud)?\s*(?P<min>\d+(?:\.\d+)?)\s*-\s*"
            r"(?P<max>\d+(?:\.\d+)?)",
            salary_text
        )
        if range_regex:
            try:
                min_val = float(range_regex.group("min"))
                max_val = float(range_regex.group("max"))
                return f"{currency}{min_val}-{max_val}{period}".strip()
            except (ValueError, TypeError):
                pass
    else:
        # Single value case
        single_regex = re.search(r"(?:[$£€]|\$aud)?\s*(?P<amount>\d+(?:\.\d+)?)", salary_text)
        if single_regex:
            try:
                amount = float(single_regex.group("amount"))
                return f"{currency}{amount}{period}".strip()
            except (ValueError, TypeError):
                pass

    return ""





                    