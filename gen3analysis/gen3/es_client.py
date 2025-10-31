from elasticsearch import Elasticsearch
from typing import Optional
from gen3analysis.filters.es.nesting_registry import NestingRegistry
from functools import lru_cache
from gen3analysis.config import logger
from gen3analysis.settings import settings
import socket
from urllib.parse import urlparse


def get_ip_address_form_of_es_host(url):
    """
    Get the IP address of a URL

    Args:
        url: The URL (e.g., 'https://www.google.com' or 'google.com')

    Returns:
        IP address as a string
    """
    # Parse the URL to extract the hostname
    parsed_url = urlparse(url)
    hostname = parsed_url.netloc if parsed_url.netloc else parsed_url.path

    # Remove port if present
    hostname = hostname.split(":")[0]
    port = parsed_url.port

    try:
        # Get the IP address
        ip_address = socket.gethostbyname(hostname)
        return f"http://{ip_address}:{port}"
    except socket.gaierror as e:
        return f"Error: Could not resolve hostname - {e}"


hosts = [h.strip() for h in settings.GEN3_ES_ENDPOINT.split(",") if h.strip()]

INDEX_LIST = [
    settings.ES_GENE_CENTRIC_INDEX,
    settings.ES_CASE_CENTRIC_INDEX,
    settings.ES_CASE_INDEX,
    settings.ES_FILE_INDEX,
    settings.ES_PROJECT_INDEX,
    settings.ES_SSM_CENTRIC_INDEX,
    settings.ES_SSM_OCCURRENCE_INDEX,
]


@lru_cache
def get_es() -> Elasticsearch:
    logger.info(f"Setting up connection to ES: {hosts}")
    # resolve the hostname to its ip address
    resolved_hosts = get_ip_address_form_of_es_host(hosts[0])
    logger.info(f"Resolved IP address for {hosts[0]} to {resolved_hosts}")

    kwargs = {"hosts": [resolved_hosts], "request_timeout": 45}
    return Elasticsearch(**kwargs)


@lru_cache
def get_nested_registry() -> dict:
    es = get_es()
    registry = {}
    for index in INDEX_LIST:
        registry[index] = NestingRegistry.build(es, index)

    return registry


def open_pit(index: str, keep_alive: Optional[str] = None) -> str:
    es = get_es()
    keep_alive = keep_alive or settings.ES_PIT_KEEP_ALIVE
    resp = es.open_point_in_time(index=index, keep_alive=keep_alive)
    return resp["id"]


def close_pit(pit_id: str) -> None:
    es = get_es()
    try:
        es.close_point_in_time(body={"id": pit_id})
    except Exception:
        # If it already expired or closed, ignore.
        pass
