import json
import asyncio
from urllib.parse import urlencode, urlparse, urlunparse, parse_qs
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, LLMConfig, CacheMode, BrowserConfig
from crawl4ai.extraction_strategy import LLMExtractionStrategy
from core.job_detail_model import JobData
from utils.date_parser import parse_posted_date

# NEW: Playwright helpers for dynamic sites
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# -------------------------------
# LLM-based list extractor (static HTML -> list)
# -------------------------------
async def job_list_extractor(url: dict, provider: str, api_token: str = None, extra_headers: dict = None):
    """Existing list extractor kept as-is for static pages.
    Accepts url dict: {"url": str, "wait_for": str}
    Returns a list of jobs with minimal fields.
    """
    browser_config = BrowserConfig(headless=True, text_mode=True)
    extra_args = {"temperature": 0, "top_p": 0.9, "max_tokens": 2000}
    if extra_headers:
        extra_args["extra_headers"] = extra_headers

    crawler_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        word_count_threshold=1,
        page_timeout=80000,
        wait_for=url.get("wait_for") or "css:div",
        extraction_strategy=LLMExtractionStrategy(
            llm_config=LLMConfig(provider=provider, api_token=api_token),
            schema=JobData.model_json_schema(),
            extraction_type="schema",
            instruction="""
            Extract the following job-related fields from the HTML content of the job page:
            jobId, title, applicationUrl, postedDate, companyName and numberOfPages

            Return result as a Python dictionary with matching keys. Use empty string or default if not found.
            """,
            extra_args=extra_args,
        ),
    )

    async with AsyncWebCrawler(config=browser_config) as crawler:
        result = await crawler.arun(url=url["url"], config=crawler_config)
        jobs = []
        content = result.extracted_content
        if isinstance(content, str):
            try:
                content = json.loads(result.extracted_content)
            except Exception:
                content = []

        if isinstance(content, list):
            for job in content:
                raw_date = job.get('postedDate', '')
                job['postedDate'] = parse_posted_date(raw_date)
                jobs.append(job)
        elif isinstance(content, dict):
            raw_date = content.get('postedDate', '')
            content['postedDate'] = parse_posted_date(raw_date)
            jobs = [content]
        else:
            jobs = []   
        return jobs

# -------------------------------
# Detail extractor from URL (kept, narrowed to relevant domains)
# -------------------------------
async def job_detail_extractor_from_url(url: str, provider: str, api_token: str = None, extra_headers: dict = None):
    browser_config = BrowserConfig(headless=True, text_mode=True)
    extra_args = {"temperature": 0, "top_p": 0.9, "max_tokens": 2000}
    if extra_headers:
        extra_args["extra_headers"] = extra_headers

    crawler_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        word_count_threshold=1,
        page_timeout=80000,
        wait_for="p, h2, h5, span",
        extraction_strategy=LLMExtractionStrategy(
            llm_config=LLMConfig(provider=provider, api_token=api_token),
            schema=JobData.model_json_schema(),
            extraction_type="schema",
            instruction="""
            Extract the following job-related fields from the HTML content of the job page and return the result as a Python dictionary with matching keys:

            Fields:
            - jobId
            - title
            - description
            - location
            - country
            - state
            - city
            - jobType
            - salary
            - skills
            - experienceLevel
            - currency
            - applicationUrl
            - benefits
            - jobStatus
            - responsibilities
            - workSettings
            - roleCategory
            - qualifications
            - companyLogo
            - companyName
            - minSalary
            - maxSalary
            - postedDate
            - category

            Instructions:
            - Only extract jobs related to Dynamics 365, Dynamics CRM, Power Apps, Dataverse, Power BI, Power Automate, canvas app, model-driven app, RPA, or Power Platform. If unrelated, return nothing.
            - Use full names for location abbreviations (e.g., "US" → "United States", "NJ" → "New Jersey").
            Return clean, structured values:
            - location, country, state, city: must contain only names, no additional text.
            - salary: If both annual and hourly rates are present, use the annual rate (“per year”). If only hourly is provided, append “per hour.” If only annual or “per year” appears, append “per year.” If no salary is found, return an empty string. If frequency isn’t specified, treat values > 10000 as “per year,” otherwise “per hour.”
            - jobType: one of "fullTime", "partTime", "contractToHire", "tempContract", "gigWork".
            - experienceLevel: one of "beginner", "intermediate", "expert", or "experienced".
            - workSettings: one of "remote", "onSite", "hybrid".
            - category: classify based on title/description as "developer", "consultant", "sales", "administrator", "architect", "analytics", "automation", "" or "engineer".
            - roleCategory: infer based on job title or context.
            
            Return the result as a single Python dictionary.
            """,
            extra_args=extra_args,
        ),
    )
# If both annual and hourly rates are present, return the annual. If only hourly is provided, return it with its frequency (e.g., "60 - 85 USD per hour"). If no salary is found, return an empty string. If the salary is anunally or anything that state per year return per year and per year and for job without the frequency if it's above 10k return per year 
    async with AsyncWebCrawler(config=browser_config) as crawler:
        try:
            result = await crawler.arun(url=url, config=crawler_config)
            content = result.extracted_content

            if isinstance(content, str):
                try:
                    parsed = json.loads(content)
                    if isinstance(parsed, str):
                        parsed = json.loads(parsed)
                    content = parsed
                except Exception:
                    return {}

            if isinstance(content, list):
                last_job = content[-1]
                raw_date = last_job.get('postedDate', '')
                last_job['postedDate'] = parse_posted_date(raw_date)
                return last_job
            elif isinstance(content, dict):
                raw_date = content.get('postedDate', '')
                content['postedDate'] = parse_posted_date(raw_date)
                return content
            else:
                return {}
        except Exception:
            return {}

# -------------------------------
# NEW: Keyword-first search + pagination + dynamic modal support
# -------------------------------
async def _apply_query_param(url: str, param: str, value: int) -> str:
    parsed = urlparse(url)
    q = parse_qs(parsed.query)
    q[param] = [str(value)]
    new_query = urlencode({k: v[0] for k, v in q.items()}, doseq=False)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))

async def _collect_static_page(url: str, wait_for: str, provider: str, api_token: str):
    return await job_list_extractor({"url": url, "wait_for": wait_for}, provider, api_token)

async def _search_and_paginate_with_playwright(site: dict, keyword: str) -> list:
    
    """Use Playwright to:
    - Go to search URL or type keyword
    - Handle pagination (next button, infinite scroll)
    - Collect job HREFs (static) OR open each card, scrape modal to find the apply/detail URL
    Returns a list of lightweight job dicts: {title, applicationUrl, postedDate, companyName}
    """
    print("I am in dynamics part....💦")
    results = []
    max_pages = int(site.get("max_pages") or 10)  # guardrail only
    wait_for = site.get("wait_for") or "body"

    search_url_template = site.get("search_url_template")
    start_url = site.get("url") 
    if search_url_template:
        start_url = search_url_template.replace("{q}", keyword)

    job_link_selector = site.get("job_link_selector")
    job_card_selector = site.get("job_card_selector")
    results_container_selector = site.get("results_container_selector") or job_link_selector or job_card_selector or "body"

    pagination_param = site.get("pagination_param")
    pagination_next_selector = site.get("pagination_next_selector")
    infinite_scroll = str(site.get("infinite_scroll") or "false").lower() == "true"

    search_input_selector = site.get("search_input_selector")
    search_submit_selector = site.get("search_submit_selector")
    search_enter = str(site.get("search_enter") or "true").lower() == "true"

    modal_selector = site.get("modal_selector")
    modal_apply_link_selector = site.get("modal_apply_link_selector")
    modal_close_selector = site.get("modal_close_selector")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        async def ensure_results_loaded():
            try:
                await page.wait_for_selector(results_container_selector, state="visible", timeout=20000)
            except PlaywrightTimeoutError:
                pass

        async def list_current_page_jobs():
            # Prefer explicit job links when provided
            if job_link_selector:
                hrefs = await page.eval_on_selector_all(job_link_selector, "els => els.map(e => e.href || e.getAttribute('href'))")
                hrefs = [h for h in (hrefs or []) if h]
                return [
                    {"title": None, "applicationUrl": h, "postedDate": "", "companyName": site.get("company_name", "")}
                    for h in hrefs
                ]
            # Otherwise use job cards (dynamic)
            if job_card_selector:
                cards = await page.query_selector_all(job_card_selector)
                print(f"Found this number of card{len(cards)}")
                out = []
                for card in cards:
                    try:
                        await card.click()
                        if modal_selector:
                            await page.wait_for_selector(modal_selector, state="visible", timeout=15000)
                        # try to extract apply/detail link within modal
                        app_url = None
                        if modal_apply_link_selector:
                            el = await page.query_selector(modal_apply_link_selector)
                            if el:
                                app_url = await el.get_attribute("href")
                        # fallback: any anchor inside modal
                        if not app_url and modal_selector:
                            try:
                                app_url = await page.eval_on_selector(modal_selector + " a[href]", "e => e.href")
                            except Exception:
                                app_url = None
                        # record
                        if app_url:
                            out.append({
                                "title": None,
                                "applicationUrl": app_url,
                                "postedDate": "",
                                "companyName": site.get("company_name", ""),
                            })
                        # close modal (optional)
                        if modal_close_selector:
                            btn = await page.query_selector(modal_close_selector)
                            if btn:
                                await btn.click()
                    except Exception:
                        continue
                return out
            return []

        # Navigate & search
        await page.goto(start_url, wait_until="domcontentloaded")
        await ensure_results_loaded()

        # If DOM-based search is required (no URL template)
        if (not search_url_template) and search_input_selector:
            try:
                await page.wait_for_selector(search_input_selector, state="visible", timeout=15000)
                await page.fill(search_input_selector, keyword)
                if search_submit_selector:
                    await page.click(search_submit_selector)
                elif search_enter:
                    await page.keyboard.press("Enter")
                await ensure_results_loaded()
            except PlaywrightTimeoutError:
                pass

        # Pagination loop
        seen_urls = set()
        page_no = 1
        while True:
            # collect
            items = await list_current_page_jobs()
            for it in items:
                url = it.get("applicationUrl")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    results.append(it)

            # advance page
            advanced = False
            if pagination_param:
                page_no += 1
                next_url = await _apply_query_param(page.url, pagination_param, page_no)
                if next_url != page.url:
                    await page.goto(next_url, wait_until="domcontentloaded")
                    await ensure_results_loaded()
                    advanced = True
            elif pagination_next_selector:
                try:
                    disabled = await page.get_attribute(pagination_next_selector, "disabled")
                    if disabled is not None:
                        advanced = False
                    else:
                        await page.click(pagination_next_selector)
                        await ensure_results_loaded()
                        advanced = True
                except Exception:
                    advanced = False
            elif infinite_scroll:
                # basic infinite scroll with guard
                prev_height = await page.evaluate("document.body.scrollHeight")
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(1200)
                new_height = await page.evaluate("document.body.scrollHeight")
                advanced = new_height > prev_height

            if not advanced:
                break
            if page_no >= max_pages:
                break

        await context.close()
        await browser.close()

    # Normalize dates
    for r in results:
        r["postedDate"] = parse_posted_date(r.get("postedDate", ""))
    return results


async def keyword_first_job_list(site: dict, keyword: str, provider: str, api_token: str) -> list:
    """High-level coordinator.
    Preference order:
    1) If `search_url_template` + `pagination_param` → static LLM list extractor across pages.
    2) Else, use Playwright to search/paginate and collect application URLs (static links or modal links).
    """
    wait_for = site.get("wait_for") or "css:div"
    results = []

    if site.get("search_url_template") and site.get("pagination_param"):
        # Query-param pagination using static extractor per page
        base = site["search_url_template"].replace("{q}", keyword)
        max_pages = int(site.get("max_pages") or 50)
        numberOfPages = None # Initialize
        for p in range(1, max_pages + 1):
            page_url = await _apply_query_param(base, site["pagination_param"], p)
            page_jobs = await _collect_static_page(page_url, wait_for, provider, api_token)
            if not page_jobs:
                break
              # Set numberOfPages only on the first page
            if p == 1:
                numberOfPages = int(page_jobs[0].get('numberOfPages')) or None
                print(f"Found {numberOfPages} of pages...✅✅")
                if numberOfPages is not None:
                    print(f"Initial page {max_pages}, now too {numberOfPages} of pages...✅✅")
                    max_pages = min(max_pages, numberOfPages)
                    print(f"Using the min {max_pages} of pages...✅✅")
            results.extend(page_jobs or [])
        # Dedup by applicationUrl
        seen = set()
        unique = []
        for j in results:
            u = j.get("applicationUrl")
            if u and u not in seen:
                seen.add(u)
                unique.append(j)
                
        print(f"Found{len(results)}, but {len(unique)} are unique")        
        return unique

    # Default to Playwright for DOM search / next-button / infinite scroll / modals
    return await _search_and_paginate_with_playwright(site, keyword)
