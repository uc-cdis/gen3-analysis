from datetime import datetime
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from fastapi import HTTPException

from gen3analysis.gen3.csrfTokenCache import CSRFTokenCache


@pytest.mark.asyncio
async def test_get_token_with_valid_cached_token():
    mock_token = "mocked_csrf_token"
    cache = CSRFTokenCache(rest_api_url="http://example.com", token_ttl_seconds=3600)

    # Use the actual CachedToken class for consistency
    from gen3analysis.gen3.csrfTokenCache import CachedToken
    from datetime import timedelta

    # Create a non-expired cached token
    cache._cached_token = CachedToken(
        token=mock_token,
        expires_at=datetime.utcnow() + timedelta(hours=1),  # Not expired
    )

    # Mock the HTTP request in case the token logic changes
    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = type(
            "Response",
            (object,),
            {
                "status_code": 200,
                "json": lambda: {"csrf": "fallback_token"},
            },
        )()

        token = await cache.get_token()

        # Should return cached token without making HTTP request
        assert token == mock_token
        mock_get.assert_not_called()


@pytest.mark.asyncio
async def test_refresh_token_raises_on_connection_error():
    cache = CSRFTokenCache(rest_api_url="http://example.com", token_ttl_seconds=3600)

    with patch(
        "httpx.AsyncClient.get", side_effect=httpx.ConnectError("Connection failed")
    ):
        with pytest.raises(HTTPException) as exc_info:
            await cache._refresh_token()

        assert exc_info.value.status_code == 503
        assert "Connection error while fetching CSRF token" in exc_info.value.detail
