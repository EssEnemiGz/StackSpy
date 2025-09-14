from typing import List, Dict
from models.sourcesPattern import SourcesPattern
import json

class Configurator:
    def __init__(self) -> None:
        pass

    def open_sources(self, path: str = "sources.json") -> Dict[str, SourcesPattern]:
        with open(path) as f:
            sources_json: Dict[str, SourcesPattern] = json.load(f)

        return sources_json

    async def get_routes(self, summoner) -> List:
        url_list = ["https://blog.mozilla.org/en", "https://softkitacademy.com", "https://hiutdenim.co.uk"]
        queue = [summoner(url) for url in url_list]
        return queue
