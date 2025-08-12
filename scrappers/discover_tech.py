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
    def html_body_parser(self, html_labels: List[str], html_body: str):
        html_key = "|".join(html_labels)
        in_html = re.search(rf"({html_key})", html_body) # Critical in resources, cause the html may be large
        return in_html

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

    async def get_html_body(self, url: str, main_session) -> str:
        logging.info(f"Starting get_html_body() in domain {url}")
        try:
            async with self.semaphore:
                async with main_session.get(url) as response:
                    if response.status != 200:
                        logging.error(f"get_html_body() failed with {response.status} code")
                        return ""
                    
                    html_body = await response.text()
                    return html_body
        except Exception as e:
            logging.error(f"Failed get_html_body() with exception {e}")
            return ""

async def main(url: str) -> bool:
    start_time = time.time()
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status != 200:
                return False

            for key, value in sources_json.items():
                results.setdefault(key, {"confidence":0} )
                actual_results = results[key] 
                # Check the headers
                headers = value.get("headers")
                actual_results["headers"] = False
                header_values = " ".join(response.headers.values()).lower()
                confidence = False
                for info in headers.values():
                    if re.search(info, header_values):
                        confidence = True
                        break
                
                if confidence:
                    actual_results["headers"] = True

                # Check the cookies
                cookies = value.get("cookies")
                if len(response.cookies) > 0:
                    actual_results["cookies"] = False
                    cookie_values = list(response.cookies.values())
                    confidence = any(cookie in cookie_values for cookie in cookies)
                    
                    if confidence:
                        actual_results["cookies"] = True

                else:
                    actual_results["cookies"] = True

                # Confidence
                confidence = value.get("confidence_weights")
                if actual_results.get("html"):
                    actual_results["confidence"] += confidence.get("html")

                if actual_results.get("headers"):
                    actual_results["confidence"] += confidence.get("headers")

                if actual_results.get("cookies"):
                    actual_results["confidence"] += confidence.get("cookies")

    end_time = time.time()
    print("Execution time: ", f"{end_time-start_time:.2f} seconds")
    return True

async def summoner():
    url = "https://blog.mozilla.org/en"
    semaphore = asyncio.Semaphore(20)
    network = Networking(semaphore)
    checker = Analyzer()
    route_tasks = []
    html_body_results = []

    async with aiohttp.ClientSession() as session:
        html_body = await network.get_html_body(url, session)
        for tech, info in sources_json.items():
            paths = info.get("routes", [])
            html_labels = info.get("html", [])
            
            analized_html = checker.html_body_parser(html_labels, html_body)
            if analized_html:
                html_body_results.append([True, tech])

            for path in paths:
                route_tasks.append(network.fetch(url+path, session, tech))

        routes_result = await asyncio.gather(*route_tasks)
        print(routes_result)
        print(html_body_results)
    print(json.dumps(results, indent=4))

if __name__ == "__main__": 
    asyncio.run(summoner())
