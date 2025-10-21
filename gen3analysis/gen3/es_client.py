from elasticsearch import Elasticsearch

from gen3analysis.filters.es.nesting_registry import NestingRegistry
from gen3analysis.settings import settings
from functools import lru_cache
from typing import Optional, Dict, List
from elasticsearch_dsl import connections
from gen3analysis.settings import settings

hosts = [h.strip() for h in settings.ES_HOSTS.split(",") if h.strip()]
connections.create_connection(hosts=hosts, timeout=45, use_ssl=settings.ES_VERIFY_SSL)


@lru_cache
def get_es() -> Elasticsearch:

    kwargs = {"hosts": hosts, "use_ssl": settings.ES_VERIFY_SSL, "request_timeout": 45}
    return Elasticsearch(**kwargs)


@lru_cache
def get_nested_registry() -> dict:
    fieldsByIndex = {
        f"{settings.ES_GENE_CENTRIC_INDEX}": [
            "case.ssm.consequence.transcript.annotation.vep_impact",
            "case.ssm.consequence.transcript.annotation.sift_impact",
            "case.ssm.consequence.transcript.annotation.polyphen_impact",
            "case.ssm.consequence.transcript.consequence_type",
            "case.ssm.mutation_subtype",
            "case.ssm.observation.observation_id",
            "case.cnv.cnv_change_5_category",
        ]
    }
    es = get_es()
    registry = {}
    for index in fieldsByIndex.keys():
        fields = fieldsByIndex[index]
        registry[index] = NestingRegistry.build(es, index, fields)

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
