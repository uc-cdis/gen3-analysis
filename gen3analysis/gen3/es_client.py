from concurrent.futures import ThreadPoolExecutor
from elasticsearch import Elasticsearch
from typing import Optional
from gen3analysis.filters.es.nesting_registry import NestingRegistry
from functools import lru_cache
from gen3analysis.settings import settings, logger

hosts = [h.strip() for h in settings.GEN3_ES_ENDPOINT.split(",") if h.strip()]

INDEX_LIST = [
    settings.ES_GENE_CENTRIC_INDEX,
    settings.ES_PROJECT_INDEX,
    settings.ES_SSM_CENTRIC_INDEX,
    settings.ES_SSM_OCCURRENCE_INDEX,
    settings.ES_CNV_CENTRIC_INDEX,
    settings.ES_CNV_OCCURRENCE_INDEX,
    settings.ES_CASE_CENTRIC_INDEX,
    settings.ES_CASE_INDEX,
    settings.ES_FILE_INDEX,
]

# Shared client for the nested registry (only used at startup)
_cached_es_client: Optional[Elasticsearch] = None

# Dedicated thread pool for ES queries to avoid contention with default asyncio thread pool
# Allow up to 50 concurrent ES queries
_es_thread_pool: Optional[ThreadPoolExecutor] = None


def get_es_executor() -> ThreadPoolExecutor:
    """Get or create the dedicated thread pool for ES queries."""
    global _es_thread_pool
    if _es_thread_pool is None:
        _es_thread_pool = ThreadPoolExecutor(
            max_workers=50, thread_name_prefix="es_query_"
        )
    return _es_thread_pool


def get_es() -> Elasticsearch:
    """
    Create a new Elasticsearch client for each request to avoid connection pool contention.
    Each client has its own connection pool, allowing concurrent requests to execute in parallel.
    """
    kwargs = {
        "hosts": hosts,
        "use_ssl": settings.ES_VERIFY_SSL,
        "request_timeout": settings.ES_TIMEOUT,
        "timeout": settings.ES_TIMEOUT,
        "retry_on_timeout": True,
        "maxsize": 100,  # Connection pool size per client
        "max_retries": 2,  # Reduce retries
        "retry_on_status": [429, 503],  # Only retry on these status codes
    }
    return Elasticsearch(**kwargs)


def get_cached_es() -> Elasticsearch:
    """
    Get or create a cached ES client for one-time operations like building the registry.
    This should only be used for initialization, not for request handling.
    """
    global _cached_es_client
    if _cached_es_client is None:
        logger.info(f"Setting up cached ES client for registry: {hosts}")
        _cached_es_client = get_es()
    return _cached_es_client


@lru_cache
def get_nested_registry() -> dict:
    es = get_cached_es()
    registry = {}
    for index in INDEX_LIST:
        logger.info(f"Building registry for: {index}")
        registry[index] = NestingRegistry.build(es, index)

    return registry


def open_pit(index: str, keep_alive: Optional[str] = None) -> str:
    es = get_cached_es()
    keep_alive = keep_alive or settings.ES_PIT_KEEP_ALIVE
    resp = es.open_point_in_time(index=index, keep_alive=keep_alive)
    return resp["id"]


def close_pit(pit_id: str) -> None:
    es = get_cached_es()
    try:
        es.close_point_in_time(body={"id": pit_id})
    except Exception:
        # If it already expired or closed, ignore.
        pass
