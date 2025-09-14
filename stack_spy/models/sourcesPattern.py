from typing import List, Dict, TypedDict

class SourcesPattern(TypedDict):
    html: List[str]
    routes: List[str]
    headers_patterns: Dict[str, str]
    cookies: List[str]
    confidence_weights: Dict[str, int]
