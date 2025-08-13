from typing import TypedDict, List, Dict
import logging
import asyncio
import aiohttp
import json
import time
import re

class SourcesPattern(TypedDict):
    html: List[str]
    routes: List[str]
    headers: Dict[str, str]
    cookies: List[str]
    confidence_weights: Dict[str, int]

with open("sources.json") as f:
    sources_json: Dict[str, SourcesPattern] = json.load(f)

logging.basicConfig(filename='development.log', level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S") 
results = {}

class Analyzer:
    def __init__(self, tech: str) -> None:
        self.tech = tech

    def html_body_parser(self, html_body: str, html_labels: List[str]) -> List:
        html_key = "|".join(html_labels)
        in_html = re.search(rf"({html_key})", html_body) # Critical in resources, cause the html may be large
        if in_html:
            return [self.tech, True]
        
        return [self.tech, False]

    def cookies_analizer(self, cookies: List[str], searched_cookies: List[str]) -> List:
        confidence = any(cookie in cookies for cookie in searched_cookies)
        return [self.tech, confidence]

    def headers_analizer(self, headers: Dict[str, str], searched_headers: Dict[str, str]) -> List:
        for header_value in headers.values():
            for searched_value in searched_headers.values():
                if re.search(header_value, searched_value):
                    return [self.tech, True]

        return [self.tech, False]

class Networking:
    def __init__(self, semaphore) -> None:
        self.semaphore: asyncio.locks.Semaphore = semaphore

    async def fetch(self, url: str, main_session, tech: str) -> List[str]:
        logging.info(f"Starting fetc() in domain {url}")
        try:
            async with self.semaphore:
                async with main_session.get(url) as response:
                    if response.status != 404:
                        return [url, tech]
        except Exception as e:
            logging.error(f"Failed fetch() with exception: {e}")
            return []
        return [] 

    async def make_request(self, url: str, main_session) -> Dict:
        logging.info(f"Starting make_request() in domain {url}")
        try:
            async with self.semaphore:
                async with main_session.get(url) as response:
                    if response.status != 200:
                        logging.error(f"make_request() failed with {response.status} code")
                        return {}
                    
                    result: Dict = {
                        "status": response.status,
                        "headers": dict(response.headers),
                        "cookies": {k: v.value for k, v in response.cookies.items()},
                        "body": await response.text()
                    }
                    return result
        except Exception as e:
            logging.error(f"make_request() failed with exception: {e}")
            return {}

async def summoner():
    url = "https://blog.mozilla.org/en"
    semaphore = asyncio.Semaphore(20)
    network = Networking(semaphore)
    route_tasks = []
    routes_result = []
    html_body_results = []
    headers_results = []
    cookies_results = []

    start_time = time.time()
    async with aiohttp.ClientSession() as session:
        response = await network.make_request(url, session)
        for tech, info in sources_json.items():
            checker = Analyzer(tech)
            paths = info.get("routes", [])
            html_labels = info.get("html", [])
            headers = info.get("headers", [])
            cookies = info.get("cookies", [])
            
            # Check the HTML
            analized_html = checker.html_body_parser(response.get("body", ""), html_labels)
            html_body_results.append(analized_html)

            # Check the headers
            analized_headers = checker.headers_analizer(response.get("headers", ""), headers)
            headers_results.append(analized_headers)

            # Check the cookies
            analized_cookies = checker.cookies_analizer(response.get("cookies", ""), cookies)
            cookies_results.append(analized_cookies)

            # Check the paths
            for path in paths:
                route_tasks.append(network.fetch(url+path, session, tech))

        routes_result = await asyncio.gather(*route_tasks)
    end_time = time.time()
    
    print(json.dumps(routes_result, indent=4))
    print(json.dumps(html_body_results, indent=4))
    print("Headers:", json.dumps(headers_results, indent=4))
    print(json.dumps(cookies_results, indent=4))
    print(f"Completed in: {end_time-start_time:.2f}s")

if __name__ == "__main__": 
    asyncio.run(summoner())
