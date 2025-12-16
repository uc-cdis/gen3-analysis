from typing import List, Optional


def normalize_csv_or_list(value: Optional[List[str]]) -> Optional[List[str]]:
    if not value:
        return value
    out: List[str] = []
    for item in value:
        if item is None:
            continue
        parts = [p.strip() for p in item.split(",")]
        out.extend([p for p in parts if p])
    return out or None
