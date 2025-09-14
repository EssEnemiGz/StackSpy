from utils.networking import Networking
from utils.analyzer import Analyzer
from utils.configurator import Configurator
from utils.cleaner import Cleaner
import logging
import asyncio
import aiohttp
import json
import time

logging.basicConfig(filename='development.log', level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S") 
configurator = Configurator()
sources_json = configurator.open_sources()

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
        queue = await configurator.get_routes(summoner)

        start_time = time.time()
        executed = await asyncio.gather(*queue, return_exceptions=True)
        end_time = time.time()
        
        cleaner = Cleaner()
        cleaned_execute = cleaner.clean_result(executed)
        print(json.dumps(cleaned_execute, indent=4))
        print(f"Completed in: {end_time-start_time:.2f}s")

    asyncio.run(main())
