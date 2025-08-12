from typing import TypedDict, List, Dict
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

results = {}
semaphore = asyncio.Semaphore(20)

class Analyzer:
    def html_body_parser(self, html_labels: List[str], html_body: str):
        html_key = "|".join(html_labels)
        in_html = re.search(rf"({html_key})", html_body) # Critical in resources, cause the html may be large
        return in_html

async def fetch(domain: str, main_session, paths: List[str], technology: str, confidence: Dict[str, int]) -> None:
    for route in paths:
        async with semaphore:
            try:
                async with main_session.get(domain+route) as response:
                    if response.status != 404:
                        results[technology]["routes"] = True
                        results[technology]["confidence"] += confidence.get("routes")
                        break
            except Exception as e:
                print(e)

async def main(url: str, domain_paths: Dict[str, List], confidences: Dict[str, Dict[str, int]]) -> bool:
    start_time = time.time()
    async with aiohttp.ClientSession() as session:
        poll_request = [fetch(domain=url, main_session=session, paths=r, technology=idx, confidence=confidences[idx]) for idx, r in domain_paths.items()]
        async with session.get(url) as response:
            if response.status != 200:
                return False

            html_body = await response.text()
            for key, value in sources_json.items():
                # Check the html
                html_key = "|".join(value.get("html"))
                in_html = re.search(rf"({html_key})", html_body) # Critical in resources, cause the html may be large

                results.setdefault(key, {"confidence":0} )
                actual_results = results[key]
                if in_html: actual_results["html"] = True

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

        await asyncio.gather(*poll_request)

    end_time = time.time()
    print("Execution time: ", f"{end_time-start_time:.2f} seconds")
    return True

if __name__ == "__main__":
    url = "https://blog.mozilla.org/en"
    domain_paths = {}
    confidences = {}
    for idx, r in sources_json.items():
        domain_paths.setdefault(idx, r.get("routes"))
        confidences.setdefault(idx, r.get("confidence_weights"))
        results.setdefault(idx, {
            "confidence": 0,
            "html": False,
            "routes": False,
            "cookies": False,
            "headers": False
        })

    asyncio.run(main(url, domain_paths, confidences))

    print(json.dumps(results, indent=4))
