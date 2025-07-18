import httpx
import asyncio
from fastapi import HTTPException
from typing import Dict, Any


class GDCGQLClient:
    def __init__(self, graphql_url: str):
        self.graphql_url = "https://portal.gdc.cancer.gov/auth/api/v0/graphql"

    def execute(
        self, query: str, variables: Dict[str, Any] = None, retry_count: int = 1
    ) -> Dict[str, Any]:
        for attempt in range(retry_count + 1):
            try:
                with httpx.Client() as client:
                    print({"query": query, "variables": variables or {}})
                    response = client.post(
                        self.graphql_url,
                        json={"query": query, "variables": variables or {}},
                        timeout=None,
                    )
                    if response.status_code != 200:
                        if attempt < retry_count:
                            continue
                        raise HTTPException(
                            status_code=response.status_code,
                            detail=f"GDC GraphQL request failed: {response.text}",
                        )
                    result = response.json()
                    return result
            except Exception as e:
                if attempt == retry_count:
                    raise
                    sleep(0.1 * (2**attempt))
