import asyncio
from typing import Dict, Any

from fastapi import Depends, HTTPException
import httpx
from starlette.status import HTTP_500_INTERNAL_SERVER_ERROR

from gen3analysis import config
from gen3analysis.config import logger
from gen3analysis.gen3.csrfTokenCache import CSRFTokenCache


class GuppyGQLClient:
    def __init__(self, graphql_url: str):
        self.graphql_url = graphql_url
        self.csrf_cache = CSRFTokenCache(
            rest_api_url=f"{config.HOSTNAME}/_status",
            token_ttl_seconds=3600,  # 1 hour
        )

    async def execute(
        self,
        access_token: str,
        query: str,
        variables: Dict[str, Any] = None,
        retry_count: int = 1,
    ) -> Dict[str, Any]:
        for attempt in range(retry_count + 1):
            try:
                csrf_token = await self.csrf_cache.get_token()
                headers = {
                    "Content-Type": "application/json",
                    "X-CSRF-Token": csrf_token,
                }
                if access_token:
                    headers["Authorization"] = f"Bearer {access_token}"
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

                    if result.get("errors"):
                        err_msg = f"GuppyGQLClient error: {result['errors']}"
                        logger.error(err_msg)
                        raise HTTPException(
                            status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=err_msg,
                        )

                    return result

            except HTTPException as e:
                logger.error(e.detail)
                raise e
            except Exception as e:
                if attempt == retry_count:
                    raise
                await asyncio.sleep(0.1 * (2**attempt))  # Exponential backoff

    def _is_csrf_error(self, result: Dict[str, Any]) -> bool:
        errors = result.get("errors", [])
        return any("csrf" in str(error).lower() for error in errors)
