  
import json
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, LLMConfig, CacheMode, BrowserConfig
from crawl4ai.extraction_strategy import LLMExtractionStrategy
from core.job_detail_model import JobData
from utils.date_parser import parse_posted_date



# ------ Job extractor --- 

async def job_list_extractor(url: dict, provider: str, api_token: str = None, extra_headers: dict = None):
    browser_config = BrowserConfig(headless=True,text_mode=True )
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
            jobId, title, applicationUrl, postedDate, companyName
            
            Return result as a Python dictionary with matching keys. Use empty string or default if not found.
            """,
            extra_args=extra_args,
        ),
    )

    async with AsyncWebCrawler(config=browser_config) as crawler:
        result = await crawler.arun(url=url["url"], config=crawler_config)
        jobs=[]
        content = result.extracted_content
        if isinstance(content, str):
            try:
                content= json.loads(result.extracted_content)
                # print("str.....")
            except Exception:
                content = []
                
        if isinstance(content, list):
            print(f" ✓✅ found {len(content)} number of jobs")
            for job in content:
                
                raw_date = job.get('postedDate', '')
                job['postedDate'] = parse_posted_date(raw_date)
                jobs.append(job)
        else:
            raw_date = content.get('postedDate', '')
            content['postedDate'] = parse_posted_date(raw_date)
            jobs = [content]
        return jobs
        
       
 
# --- Extract Structured Data from a URL ---
async def job_detail_extractor_from_url(url:str, provider: str, api_token: str = None, extra_headers: dict = None):
    browser_config = BrowserConfig(headless=True,text_mode=True )
    extra_args = {"temperature": 0, "top_p": 0.9, "max_tokens": 2000}
    if extra_headers:
        extra_args["extra_headers"] = extra_headers

    crawler_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        word_count_threshold=1,
        page_timeout=80000,
        # wait_for="css:h1",
        wait_for = "css:h1, p, h5, span",
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
            - Only extract jobs related to Dynamics 365 or Power Platform. If unrelated, return nothing.
            - Use full names for location abbreviations (e.g., "US" → "United States", "NJ" → "New Jersey").
            Return clean, structured values:
            - location, country, state, city: must contain only names, no additional text.
            - salary: If both annual and hourly rates are present, return the annual. If only hourly is provided, return it with its frequency (e.g., "60 - 85 USD per hour"). If no salary is found, return an empty string.
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
    async with AsyncWebCrawler(config=browser_config) as crawler:
        
        try: 
            result = await crawler.arun(url=url, config=crawler_config)
            content = result.extracted_content
        
            last_job={}

            # Attempt to parse string to JSON
            if isinstance(content, str):
                try:
                    # json string
                    parsed = json.loads(content)
                    if isinstance(parsed, str):
                        parsed = json.loads(parsed)
                    content=parsed
                    print("✓✅ JSON string parsed.")
                    # print("dictionary", job_dic)
                except Exception as e:
                    print("Failed to parse JSON string.", e)
                    return {}
                
                
            # If content is a list of jobs
                if isinstance(content, list):
                
                    print(f"✓✅ Found {len(content)} job(s)")
                    last_job=content[-1]
                    raw_date = last_job.get('postedDate', '')
                    last_job['postedDate'] = parse_posted_date(raw_date)
                    print("✓✅ Extracted job:")
                    return last_job

                # If content is a single job dictionary
                elif isinstance(content, dict):
                    raw_date = content.get('postedDate', '')
                    content['postedDate'] = parse_posted_date(raw_date)
                    #   content
                    # print("✓✅ Single job (dict):")
                    return content
                    
                else:
                    # print(f"⚠️ Unexpected parsed content type: {type(content)}")
                    return {}    
        except Exception as e:
            return {}    

    
   
