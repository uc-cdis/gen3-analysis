from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from elasticsearch import Elasticsearch
from fastapi import APIRouter, Depends, HTTPException, Query
from starlette import status
from starlette.responses import JSONResponse
import json
from pydantic import BaseModel
from gen3analysis.filters.gen3GQLFilters import parse_gql_filter
from gen3analysis.query_builders.genomic.ssm import ssm_table_query

from gen3analysis.settings import settings
from gen3analysis.gen3.es_client import open_pit, get_es
from gen3analysis.gen3.cursor import encode_cursor, decode_cursor
from gen3analysis.utils.filters import project_filter
from gen3analysis.models.genes import (
    TopGenesResponse,
    GeneBucket,
)

from gen3analysis.query_builders.genomic.queries import (
    query_top_genes,
    gene_table_query,
)

genomic = APIRouter()


def _build_agg_body(
    size: int,
    after_key: Optional[Dict[str, Any]],
    project: Optional[str],
    pit_id: str,
) -> Dict[str, Any]:
    """
    Build a search body with:
      - size: 0 (aggregation only)
      - composite agg 'by_gene' over TOP_GENES_GENE_ID_FIELD
      - nested sub-agg to cardinality count distinct cases
    """
    filters = project_filter(project)

    body: Dict[str, Any] = {
        "size": 0,
        "query": {"bool": {"filter": filters}} if filters else {"match_all": {}},
        "aggs": {
            "by_gene": {
                "composite": {
                    "size": size,
                    "sources": [
                        {
                            "gene_id": {
                                "terms": {"field": settings.TOP_GENES_GENE_ID_FIELD}
                            }
                        }
                    ],
                    **({"after": after_key} if after_key else {}),
                },
                "aggs": {
                    "case_nested": {
                        "nested": {"path": settings.TOP_GENES_CASE_NESTED_PATH},
                        "aggs": {
                            "unique_cases": {
                                "cardinality": {
                                    "field": settings.TOP_GENES_CASE_ID_FIELD,
                                    "precision_threshold": 40000,
                                }
                            }
                        },
                    }
                },
            }
        },
        # Use PIT for consistency across pages. For searches with only aggs,
        # sort/_shard_doc isn't required, but harmless if included.
        "pit": {"id": pit_id, "keep_alive": settings.ES_PIT_KEEP_ALIVE},
        "track_total_hits": False,
    }
    return body


def fetch_top_genes_page(
    es: Elasticsearch,
    size: int,
    project: Optional[str],
    cursor: Optional[str],
    keep_alive_override: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Returns (items, next_cursor)
    items: list of {gene_id, case_count, doc_count}
    next_cursor: opaque cursor with pit_id + after_key or None if last page
    """
    if cursor:
        decoded = decode_cursor(cursor)
        pit_id = decoded["pit_id"]
        after_key = decoded.get("after_key")
    else:
        pit_id = open_pit(
            settings.TOP_GENES_INDEX, keep_alive_override or settings.ES_PIT_KEEP_ALIVE
        )
        after_key = None

    body = _build_agg_body(
        size=size, after_key=after_key, project=project, pit_id=pit_id
    )

    json_query = json.dumps(body)
    # write the query to a file
    with open(
        f"./logs/es_query.json",
        "w",
    ) as f:
        f.write(json_query)

    resp = es.search(body=body)

    json_resp = json.dumps(resp)
    print(json_resp)

    buckets = resp["aggregations"]["by_gene"]["buckets"]
    next_after = resp["aggregations"]["by_gene"].get("after_key")

    items: List[Dict[str, Any]] = []
    for b in buckets:
        gene_id = b["key"]["gene_id"]
        doc_count = b["doc_count"]
        case_count = b["case_nested"]["unique_cases"]["value"]
        items.append(
            {
                "gene_id": gene_id,
                "case_count": int(case_count),
                "doc_count": int(doc_count),
            }
        )

    if next_after:
        next_cursor = encode_cursor(pit_id, next_after)
    else:
        # No more pages; let PIT expire naturally (or you can explicitly close here if you prefer stateful tracking)
        next_cursor = None

    return items, next_cursor


@genomic.post(
    path="/top-by-cases",
    status_code=status.HTTP_200_OK,
    description="Performs a cohort query and return the query for all items matching the ids.",
    summary="Queries for cohort ids and uses those ids as the cohort in the second query",
    responses={
        status.HTTP_200_OK: {"description": "Successfully processed the cohort query"},
        status.HTTP_400_BAD_REQUEST: {
            "description": "The request body is missing required fields or has invalid values."
        },
        status.HTTP_401_UNAUTHORIZED: {
            "description": "User unauthorized when accessing endpoint"
        },
        status.HTTP_403_FORBIDDEN: {
            "description": "User does not have access to requested data"
        },
        status.HTTP_500_INTERNAL_SERVER_ERROR: {
            "description": "Something went wrong internally when processing the request"
        },
    },
)
def top_genes(
    project: str | None = Query(default=None, description="Project filter (optional)"),
    size: int = Query(default=20, ge=1, le=1000),
    cursor: str | None = Query(default=None),
    keep_alive: str | None = Query(
        default=None, description="Override PIT keep_alive (e.g., '2m')"
    ),
):
    """
    Returns top genes by distinct case count, paginated with a stable cursor.
    - If `cursor` is provided, it will continue from the previous page using the same PIT.
    - Otherwise, opens a new PIT and begins from the first page.
    """
    es = get_es()
    items, next_cursor = fetch_top_genes_page(
        es=es, size=size, project=project, cursor=cursor, keep_alive_override=keep_alive
    )
    return TopGenesResponse(
        items=[GeneBucket(**i) for i in items],
        cursor=next_cursor,
    )


class TopGeneChartRequest(BaseModel):
    cohort_filter: Optional[Dict[str, Any]] = Query(
        default=None, description="Case filter (optional)"
    )
    gene_filter: Optional[Dict[str, Any]] = Query(
        default=None, description="Gene filter (optional)"
    )
    ssm_filter: Optional[Dict[str, Any]] = Query(
        default=None, description="Mutation filter (optional)"
    )
    size: int = Query(default=20, ge=1, le=1000)
    offset: int = Query(default=0, ge=0)
    search: Optional[str] = Query(default=".*.*", description="Search term (optional)")


@genomic.post(
    path="/gene_frequency_chart",
    status_code=status.HTTP_200_OK,
    description="Returns top genes filtered by cohort, gene, and ssm filters",
    summary="Top Genes by Cohort, Gene, and SSM",
    responses={
        status.HTTP_200_OK: {"description": "Successfully processed the chart query"},
        status.HTTP_400_BAD_REQUEST: {
            "description": "The request body is missing required fields or has invalid values."
        },
        status.HTTP_401_UNAUTHORIZED: {
            "description": "User unauthorized when accessing endpoint"
        },
        status.HTTP_403_FORBIDDEN: {
            "description": "User does not have access to requested data"
        },
        status.HTTP_500_INTERNAL_SERVER_ERROR: {
            "description": "Something went wrong internally when processing the request"
        },
    },
)
@genomic.post(path="/gene_frequency_chart")
def gene_frequency_chart(body: TopGeneChartRequest):
    cohort_filter = parse_gql_filter(body.cohort_filter)
    gene_filter = parse_gql_filter(body.gene_filter)
    ssm_filter = parse_gql_filter(body.ssm_filter)

    chart_data = query_top_genes(
        case_filter=cohort_filter,
        gene_filter=gene_filter,
        ssm_filter=ssm_filter,
    )
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=chart_data,
    )


@genomic.post(
    path="/gene_table",
    status_code=status.HTTP_200_OK,
    description="Returns pages gene frequency table filtered by cohort, gene, and ssm filters",
    summary="Mutation Frequency: Gene Table",
    responses={
        status.HTTP_200_OK: {"description": "Successfully processed the table query"},
        status.HTTP_400_BAD_REQUEST: {
            "description": "The request body is missing required fields or has invalid values."
        },
    },
)
@genomic.post(path="/gene_table")
def gene_table(body: TopGeneChartRequest):
    cohort_filter = parse_gql_filter(body.cohort_filter)
    gene_filter = parse_gql_filter(body.gene_filter)
    ssm_filter = parse_gql_filter(body.ssm_filter)
    size = body.size
    offset = body.offset

    table_data = gene_table_query(cohort_filter, gene_filter, ssm_filter, size, offset)

    return JSONResponse(status_code=status.HTTP_200_OK, content=table_data)


@genomic.post(
    path="/gene_table",
    status_code=status.HTTP_200_OK,
    description="Returns pages gene frequency table filtered by cohort, gene, and ssm filters",
    summary="Mutation Frequency: Gene Table",
    responses={
        status.HTTP_200_OK: {"description": "Successfully processed the table query"},
        status.HTTP_400_BAD_REQUEST: {
            "description": "The request body is missing required fields or has invalid values."
        },
    },
)
@genomic.post(path="/ssm_table")
def ssm_table(body: TopGeneChartRequest):
    cohort_filter = parse_gql_filter(body.cohort_filter)
    gene_filter = parse_gql_filter(body.gene_filter)
    ssm_filter = parse_gql_filter(body.ssm_filter)
    size = body.size
    offset = body.offset
    search = body.search

    table_data = ssm_table_query(cohort_filter, gene_filter, ssm_filter, size, offset)

    return JSONResponse(status_code=status.HTTP_200_OK, content=table_data)
