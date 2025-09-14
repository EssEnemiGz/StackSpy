from typing import List, Dict
import logging
import re

class Analyzer:
    def __init__(self, tech: str) -> None:
        self.tech = tech

    def html_body_parser(self, html_body: str, html_labels: List[str]) -> List:
        logging.info("Executing html_body_parser")
        try:
            html_key = r"|".join(re.escape(label) for label in html_labels)
            in_html = re.search(rf"({html_key})", html_body) # Critical in resources, html may be large
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
