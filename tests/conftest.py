from asgi_lifespan import LifespanManager
from httpx import AsyncClient, ASGITransport
import pytest
import pytest_asyncio

from gen3analysis.main import get_app


@pytest.fixture(scope="session")
def app():
    return get_app()


@pytest_asyncio.fixture(scope="function")
async def client(app):
    async with LifespanManager(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as test_client:
            yield test_client
