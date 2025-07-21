from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional
import asyncio
import httpx
from fastapi import HTTPException

from gen3analysis.config import logger


@dataclass
class CachedToken:
    token: str
    expires_at: datetime

    def is_expired(self) -> bool:
        return datetime.utcnow() >= self.expires_at


class CSRFTokenCache:
    def __init__(self, rest_api_url: str, token_ttl_seconds: int = 3600):
        self.rest_api_url = rest_api_url
        self.token_ttl = timedelta(seconds=token_ttl_seconds)
        self._cached_token: Optional[CachedToken] = None
        self._lock = asyncio.Lock()
        logger.info("CSRFTokenCache initialized")

    async def get_token(self) -> str:
        async with self._lock:
            if self._cached_token is None or self._cached_token.is_expired():
                await self._refresh_token()
            return self._cached_token.token

    async def _refresh_token(self):
        try:
            async with httpx.AsyncClient() as session:
                response = await session.get(self.rest_api_url)
                if response.status_code != 200:
                    raise HTTPException(
                        status_code=500, detail="Failed to fetch CSRF token"
                    )

                data = response.json()
                token = data.get("csrf")  # get token from response

                if not token:
                    raise HTTPException(
                        status_code=500, detail="CSRF token not found in response"
                    )

                self._cached_token = CachedToken(
                    token=token, expires_at=datetime.utcnow() + self.token_ttl
                )
        except httpx.ConnectError as e:
            raise HTTPException(
                status_code=503,
                detail=f"Connection error while fetching CSRF token: {str(e)}",
            )
        except httpx.TimeoutException as e:
            raise HTTPException(
                status_code=504, detail=f"Timeout while fetching CSRF token: {str(e)}"
            )
        except httpx.RequestError as e:
            raise HTTPException(
                status_code=500, detail=f"Error fetching CSRF token: {str(e)}"
            )
