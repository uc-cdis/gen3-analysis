from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class TopGenesQuery(BaseModel):
    project: Optional[str] = Field(
        default=None, description="Filter by project_id if present in mappings"
    )
    size: int = Field(default=20, ge=1, le=1000, description="Page size for buckets")
    cursor: Optional[str] = None
    keep_alive: Optional[str] = Field(
        default=None, description="Override PIT keep_alive like '1m', '5m'"
    )


class GeneBucket(BaseModel):
    gene_id: str
    case_count: int
    doc_count: int


class TopGenesResponse(BaseModel):
    items: List[GeneBucket]
    cursor: Optional[str] = None  # pass this back to fetch next page
