import httpx
import asyncio
from fastapi import HTTPException
from typing import Dict, Any

from gen3analysis.gen3.auth import Gen3AuthToken
from gen3analysis.gen3.csrfTokenCache import CSRFTokenCache


class GuppyGQLClient:
    def __init__(
        self,
        graphql_url: str,
        csrf_cache: CSRFTokenCache,
        gen3_auth_token: Gen3AuthToken,
    ):
        self.graphql_url = graphql_url
        self.csrf_cache = csrf_cache
        self.gen3_auth_token = gen3_auth_token

    async def execute(
        self, query: str, variables: Dict[str, Any] = None, retry_count: int = 1
    ) -> Dict[str, Any]:
        for attempt in range(retry_count + 1):
            try:
                csrf_token = await self.csrf_cache.get_token()
                gen3_auth_token = await self.gen3_auth_token.get_access_token()
                headers = {
                    "Content-Type": "application/json",
                    "X-CSRF-Token": csrf_token,
                    "Authorization": f"Bearer {gen3_auth_token}",
                }
                payload = {"query": query, "variables": variables or {}}
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        self.graphql_url, json=payload, headers=headers
                    )

                    if response.status_code != 200:
                        if attempt < retry_count:
                            continue
                        raise HTTPException(
                            status_code=response.status_code,
                            detail=f"GraphQL request failed: {response.text}",
                        )

                    result = response.json()

                    # Check for CSRF-related errors
                    if self._is_csrf_error(result):
                        if attempt < retry_count:
                            await self.csrf_cache._refresh_token()  # Force refresh
                            continue

                    return result

            except Exception as e:
                if attempt == retry_count:
                    raise
                await asyncio.sleep(0.1 * (2**attempt))  # Exponential backoff

    def _is_csrf_error(self, result: Dict[str, Any]) -> bool:
        errors = result.get("errors", [])
        return any("csrf" in str(error).lower() for error in errors)
