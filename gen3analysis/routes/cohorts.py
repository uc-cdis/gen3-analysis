from typing import Dict, Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi import Cookie
from glom import glom
from pydantic import BaseModel
from starlette import status
from starlette.responses import JSONResponse

from gen3analysis.auth import Auth
from gen3analysis.config import logger
from gen3analysis.dependencies.guppy_client import get_guppy_client
from gen3analysis.gen3.guppyQuery import GuppyGQLClient
from gen3analysis.routes import cases
import json
from collections import defaultdict

cohorts = APIRouter()

TOP_GENES_QUERY = """
query topGeneCases($filters: JSON, $geneFilters: JSON, $numGenes: Int) {
    CaseCentric__aggregation {
        case_centric(filter: $filters) {
            _totalCount
            gene {
                gene_id {
                    histogram {
                        key
                        count
                    }
                }
            }
        }
    }
    Gene_gene (filter: $geneFilters, first: $numGenes) {
        gene_id
        symbol
        name
        cytoband
        biotype
        is_cancer_gene_census
    }
}
"""


class CohortQueryRequest(BaseModel):
    cohort_filters: Dict
    filters: Dict = {}
    query: str = ""
    case_index: str = ""
    cohort_item_field: str = ""
    limit: int = 10000


@cohorts.post(
    path="/query",
    dependencies=[Depends(get_guppy_client)],
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
async def cohort_query(
    body: CohortQueryRequest,
    access_token: Optional[str] = Cookie(None),
    gen3_graphql_client: GuppyGQLClient = Depends(get_guppy_client),
    auth: Auth = Depends(Auth),
) -> JSONResponse:
    cohort_filters = body.cohort_filters
    case_index = body.case_index
    cohort_item_field = body.cohort_item_field
    limit = body.limit
    query = body.query
    filters = body.filters

    if cohort_filters is None == 0:
        raise HTTPException(status_code=400, detail="Must have the cohort_query filter")

    if filters is None or len(filters) == 0:
        raise HTTPException(status_code=400, detail="Must have the query filter")

    if query is None or len(filters) == 0:
        raise HTTPException(status_code=400, detail="Must have the query")

    try:
        data = await cases.cohort_query(
            gen3_graphql_client=gen3_graphql_client,
            case_index=case_index,
            cohort_item_field=cohort_item_field,
            query=query,
            cohort_filters=cohort_filters,
            filters=filters,
            limit=limit,
            access_token=access_token,
        )

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "results": data,
            },
        )
    except Exception as e:
        logger.error(f"Error while processing cohort query: {e}")
        raise HTTPException(status_code=500, detail="Error with cohort query")


class TopGenesChartRequest(BaseModel):
    cohort_filters: Dict = {"and": []}
    gene_filters: Dict = {"and": []}
    ssm_filters: Dict = {
        "and": [
            {
                "nested": {
                    "path": "case.project",
                    "in": {"project_id": ["MMRF-COMMPASS"]},
                }
            }
        ]
    }
    size: int = 20


@cohorts.post(
    path="/top_genes_in_cohort",
)
async def top_genes_in_cohort(
    body: TopGenesChartRequest,
    access_token: Optional[str] = Cookie(None),
    gen3_graphql_client: GuppyGQLClient = Depends(get_guppy_client),
) -> JSONResponse:
    # get the first N genes filtered by the gene query (usually is_gene_cancer_census = true)

    case_filters = body.cohort_filters
    if (case_filters is None) or (case_filters.get("and") is None):
        case_filters = {"and": []}

    case_filters["and"].append({"in": {"available_variation_data": ["ssm"]}})
    # case_filters["and"].append(
    #     {"nested": {"path": "gene", "eq": {"is_cancer_gene_census": True}}}
    # )

    ssm_filters = body.ssm_filters
    if (ssm_filters is None) or (ssm_filters.get("and") is None):
        ssm_filters = {"and": []}

    gene_filters = body.gene_filters
    if (gene_filters is None) or (gene_filters.get("and") is None):
        gene_filters = {"and": []}

    case_data = await cases.get_item_ids(
        gen3_graphql_client=gen3_graphql_client,
        item_fields=["case_id"],
        doc_type="CaseCentric_case_centric",
        guppy_filter=case_filters,
        access_token=access_token,
    )

    if (case_data.get("data") is None) or (
        case_data.get("data").get("CaseCentric_case_centric") is None
    ):
        return JSONResponse(
            status_code=status.HTTP_200_OK, content={"hits": [], "total": 0}
        )
    case_ids = [
        glom(x, "case_id") for x in glom(case_data, f"data.CaseCentric_case_centric")
    ]

    # print(
    #     "case ids",
    #     json.dumps({"nested": {"path": "case", "in": {"case_id": case_ids}}}),
    # )

    # build a filter containing the cohort ids and merge with the other filters
    gene_filters["and"].append({"eq": {"is_cancer_gene_census": True}})
    # gene_filters = {
    #     "and": [
    #         {"eq": {"is_cancer_gene_census": True}}
    #         #  ,  {"nested": {"path": "case", "in": {"case_id": case_ids}}}
    #     ]
    # }
    gene_data = await cases.get_item_ids(
        gen3_graphql_client=gen3_graphql_client,
        item_fields=["gene_id"],
        doc_type="Gene_gene",
        guppy_filter=gene_filters,
        access_token=access_token,
    )

    gene_index_gene_ids = [
        glom(x, "gene_id") for x in glom(gene_data, f"data.Gene_gene")
    ]

    ssm_filters["and"].append(
        {
            "nested": {
                "path": "consequence.transcript",
                "nested": {
                    "path": "consequence.transcript.gene",
                    "in": {"gene_id": gene_index_gene_ids},
                },
            }
        }
    )
    # ssm_filters["and"].append(
    #     {
    #         "nested": {
    #             "path": "occurrence.case",
    #             "in": {"case_id": case_ids},
    #         },
    #     }
    # )

    # save query and variables to a file for debugging

    # use case id and gene filters to get a list of genes

    ssm_data = await cases.get_item_ids(
        gen3_graphql_client=gen3_graphql_client,
        doc_type="Ssm_ssm",
        item_fields=["occurrence.case.case_id", "consequence.transcript.gene.gene_id"],
        guppy_filter=ssm_filters,
        access_token=access_token,
    )

    if (ssm_data.get("data") is None) or (ssm_data.get("data").get("Ssm_ssm") is None):
        return JSONResponse(
            status_code=status.HTTP_200_OK, content={"hits": [], "total": 0}
        )

    case_set = set(case_ids)

    ssm_counts = defaultdict(set)
    for x in glom(ssm_data, f"data.Ssm_ssm"):
        gene_id = glom(x["consequence"][0], "transcript.gene.gene_id")
        # Pre-filter cases during iteration instead of creating intermediate sets
        for occurrence in x["occurrence"]:
            case_id = glom(occurrence, "case.case_id")
            if case_id in case_set:
                ssm_counts[gene_id].add(case_id)

    sorted_items = sorted(
        ssm_counts.items(), key=lambda item: len(list(item[1])), reverse=True
    )
    ssm_counts = dict(sorted_items)

    genomic_gene_ids = set()
    filtered_case_ids = set()
    gene_index_set = set(gene_index_gene_ids)

    for x in glom(ssm_data, f"data.Ssm_ssm"):
        gene_id = glom(x["consequence"][0], "transcript.gene.gene_id")
        if gene_id in gene_index_set:
            genomic_gene_ids.add(gene_id)

        for occurrence in x["occurrence"]:
            case_id = glom(occurrence, "case.case_id")
            if case_id in case_set:
                filtered_case_ids.add(case_id)

    # print("filtered gene ids", len(list(filtered_gene_ids)))

    # print("filtered gene ids", len(list(genomic_gene_ids)))

    top_cases_query = """
   query topGeneCases($filters: JSON, $geneFilters: JSON,  $numGenes: Int) {
    CaseCentric__aggregation {
        case_centric(filter: $filters) {
        _totalCount
            gene {
                gene_id {
                    histogram {
                        key
                        count
                    }
                }
            }
        }
    }
    Gene_gene (filter: $geneFilters, first: $numGenes) {
        gene_id
        symbol
        symbol
        name
        cytoband
        biotype
        gene_id
        is_cancer_gene_census
    }
}
   """

    filter_gene_ids = list(genomic_gene_ids)

    top_gene_filters = {
        "filters": {
            "and": [
                {"nested": {"path": "gene", "in": {"gene_id": filter_gene_ids}}},
                {"in": {"case_id": list(filtered_case_ids)}},
            ]
        },
        "geneFilters": {"in": {"gene_id": filter_gene_ids}},
        "numGenes": len(filter_gene_ids),
    }

    # print("executing query", top_cases_query)
    # print("executing variables",  json.dumps(top_gene_filters, indent=2))
    # with open("topGenesQuery.json", "w") as f:
    #     f.write(json.dumps({"query": top_cases_query, "filter": top_gene_filters}, indent=2))
    chart_data = await gen3_graphql_client.execute(
        access_token=access_token, query=TOP_GENES_QUERY, variables=top_gene_filters
    )

    # filters counts by gene_id symbol

    histogram_data = glom(
        chart_data,
        "data.CaseCentric__aggregation.case_centric.gene.gene_id.histogram",
    )

    caseCount = glom(
        chart_data, "data.CaseCentric__aggregation.case_centric._totalCount"
    )

    gene_metadata = {}
    for gene in glom(chart_data, "data.Gene_gene"):
        gene_metadata[glom(gene, "gene_id")] = gene

    ordered_gene_metadata = []
    # use genomic_gene_ids to test each key of histogram_data
    for gene_key in ssm_counts.keys():
        if gene_key in genomic_gene_ids:
            if gene_metadata.get(gene_key) is not None:
                ordered_gene_metadata.append(
                    {
                        **gene_metadata.get(gene_key),
                        **{
                            "numCases": len(list(ssm_counts[gene_key])),
                            "cnv_case": 0,  # TODO remove once we have cvn data
                            "case_cnv_amplification": 0,
                            "case_cnv_loss": 0,
                            "case_cnv_gain": 0,
                            "case_cnv_homozygous_deletion": 0,
                            "ssm_case": 0,  # TODO: figure out how to compute
                        },
                    }
                )

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "data": {
                "totalGenes": len(genomic_gene_ids),
                "totalCases": caseCount,
                "genes": ordered_gene_metadata,
            }
        },
    )
