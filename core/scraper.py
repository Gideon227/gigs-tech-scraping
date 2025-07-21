import random
import requests
from fake_useragent import UserAgent
from utils.logger import logger
from config import PROXIES

def get_proxy():
    return random.choice(PROXIES)

def fetch_page(url):
    headers = {"User-Agent": UserAgent().random}
    proxy = get_proxy()

    try:
        response = requests.get(url, headers=headers, proxies={"http": proxy, "https": proxy}, timeout=15)
        response.raise_for_status()
        return response.text
    except Exception as e:
        logger.error(f"Failed to fetch {url}: {e}")
        return None