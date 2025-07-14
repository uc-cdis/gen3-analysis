import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List
from gen3analysis.filters.filters import FilterSet

from pydantic import BaseModel


class SurvivalRequest(BaseModel):
    test: List[FilterSet]
