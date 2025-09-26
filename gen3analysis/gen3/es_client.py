from elasticsearch import Elasticsearch
from gen3analysis.settings import settings
from functools import lru_cache
from typing import Optional


@lru_cache
def get_es() -> Elasticsearch:
    hosts = [h.strip() for h in settings.ES_HOSTS.split(",") if h.strip()]
    kwargs = {
        "hosts": hosts,
    }
    return Elasticsearch(**kwargs)


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
