from typing import List, Dict
import logging
import asyncio
import aiohttp

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

    async def make_request(self, url: str, main_session, timeout: float = 2) -> Dict:
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
