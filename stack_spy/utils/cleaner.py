from typing import List, Dict

class Cleaner:
    def clean_result(self, original_result: List) -> List[Dict[str, Dict[str, bool]]]:
        cleaned_list = original_result.copy()
        for item in original_result:
            if isinstance(item, Exception):
                cleaned_list.pop(cleaned_list.index(item))

        return cleaned_list
