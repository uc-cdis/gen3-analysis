from typing import List, Dict, Any, Optional
from gen3analysis.settings import settings


def project_filter(project: Optional[str]) -> List[Dict[str, Any]]:
    if not project:
        return []
    # Project is often nested under occurrence.case.* — we’ll put it in the top-level bool/filter;
    # nested agg below handles the pathing for case-level metrics.
    return [
        {
            "nested": {
                "path": settings.TOP_GENES_CASE_NESTED_PATH,
                "query": {"term": {f"{settings.TOP_GENES_PROJECT_FIELD}": project}},
                "score_mode": "none",
            }
        }
    ]
