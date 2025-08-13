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
    headers_patterns: Dict[str, str]
    cookies: List[str]
    confidence_weights: Dict[str, int]

with open("sources.json") as f:
    sources_json: Dict[str, SourcesPattern] = json.load(f)

logging.basicConfig(filename='development.log', level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S") 

class Analyzer:
    def __init__(self, tech: str) -> None:
        self.tech = tech

    def html_body_parser(self, html_body: str, html_labels: List[str]) -> List:
        logging.info("Executing html_body_parser")
        try:
            html_key = r"|".join(re.escape(label) for label in html_labels)
            in_html = re.search(rf"({html_key})", html_body) # Critical in resources, cause the html may be large
            if in_html:
                return [self.tech, True]
        
            return [self.tech, False]
        except Exception as e:
            logging.error(f"html_body_parser failed with exception: {e}")
            raise Exception(e)

    def cookies_analizer(self, cookies: List[str], cookies_patterns: List[str]) -> List:
        logging.info("Executing cookies_analizer")
        try:
            confidence = any(c_pattern.lower() in c.lower() for c in cookies for c_pattern in cookies_patterns)
            return [self.tech, confidence]
        except Exception as e:
            logging.error(f"cookies_analizer failed with exception: {e}")
            raise Exception(e)

    def headers_analizer(self, headers: Dict[str, str], searched_headers: Dict[str, str]) -> List:
        logging.info("Executing headers_analizer")
        try:
            for header_value in headers.values():
                for searched_value in searched_headers.values():
                    if re.search(searched_value, header_value):
                        return [self.tech, True]

            return [self.tech, False]
        except Exception as e:
            logging.error(f"headers_analizer failed with exception: {e}")
            raise Exception(e)

class Networking:
    def __init__(self, semaphore) -> None:
        self.semaphore: asyncio.locks.Semaphore = semaphore

    async def fetch(self, url: str, main_session, tech: str) -> List:
        logging.info(f"Starting fetc() in domain {url}")
        try:
            async with self.semaphore:
                async with main_session.get(url) as response:
                    if response.status != 404:
                        return [tech, True]
        except Exception as e:
            logging.error(f"Failed fetch() with exception: {e}")
            raise Exception(e)

        return [tech, False] 

    async def make_request(self, url: str, main_session, timeout: int = 2) -> Dict:
        logging.info(f"Starting make_request() in domain {url}")
        try:
            async with self.semaphore:
                async with main_session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
                    if response.status != 200:
                        logging.error(f"make_request() failed with {response.status} code in {url}")
                     
                    request_result: Dict = {
                        "status": response.status,
                        "headers": dict(response.headers),
                        "cookies": [v for v in response.cookies.values()],
                        "body": await response.text()
                    }
                    return request_result
        except Exception as e:
            logging.error(f"make_request() failed with exception: {e}")
            raise Exception(e)

async def summoner(url: str):
    semaphore = asyncio.Semaphore(20)
    network = Networking(semaphore)
    results = {}
    route_tasks = []
    results.setdefault(url, {})
    async with aiohttp.ClientSession() as session:
        response = await network.make_request(url, session)
        for tech, info in sources_json.items():
            results[url].setdefault(tech, {})
            actual_structure = results[url][tech]
        
            checker = Analyzer(tech)
            paths = info.get("routes", [])
            html_labels = info.get("html", [])
            headers_patterns = info.get("headers_patterns", [])
            cookies_patterns = info.get("cookies_patterns", [])
        
            # Check the HTML
            analized_html = checker.html_body_parser(response.get("body", ""), html_labels)
            actual_structure["html"] = analized_html[1]

            # Check the headers
            analized_headers = checker.headers_analizer(response.get("headers", ""), headers_patterns)
            actual_structure["headers"] = analized_headers[1]

            # Check the cookies
            analized_cookies = checker.cookies_analizer(response.get("cookies", ""), cookies_patterns)
            actual_structure["cookies"] = analized_cookies[1]

            # Check the paths
            for path in paths:
                route_tasks.append(network.fetch(url+path, session, tech))

        routes_result = await asyncio.gather(*route_tasks)
        for individual_path in routes_result:
            results[url][individual_path[0]]["routes"] = individual_path[1]

    return results

if __name__ == "__main__":
    async def main():
        url_list = ["https://blog.mozilla.org/en", "https://softkitacademy.com"]
        queue = [summoner(url) for url in url_list]

        start_time = time.time()
        executed = await asyncio.gather(*queue, return_exceptions=True)
        end_time = time.time()
        
        print(json.dumps(executed, indent=4))
        print(f"Completed in: {end_time-start_time:.2f}s")

    asyncio.run(main())
