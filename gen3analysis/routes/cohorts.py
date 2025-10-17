from typing import Dict, Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi import Cookie
from glom import glom
from pydantic import BaseModel, Field
from starlette import status
from starlette.responses import JSONResponse

from gen3analysis.auth import Auth
from gen3analysis.config import logger
from gen3analysis.dependencies.guppy_client import get_guppy_client
from gen3analysis.gen3.guppyQuery import GuppyGQLClient
from gen3analysis.query_builders.cases import cases
import json

cohorts = APIRouter()


class CohortQueryRequest(BaseModel):
    cohort_filter: Optional[Dict] = Field(
        default=None, description="case filter (optional)"
    )
    filter: Optional[Dict] = Field(default=None, description="query filter (optional)")
    query: Optional[str] = Field(default="", description="query (optional)")
    case_index: str = Field(description="case index to query")
    cohort_item_field: str = Field(description="identity field for the case")
    limit: Optional[int] = Field(
        default=10000, description="set the number of responses (optional)"
    )


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
    access_token: str | None = Cookie(default=None, alias="access_token"),
    gen3_graphql_client: GuppyGQLClient = Depends(get_guppy_client),
    auth: Auth = Depends(Auth),
) -> JSONResponse:
    cohort_filter = body.cohort_filter
    case_index = body.case_index
    cohort_item_field = body.cohort_item_field
    limit = body.limit
    query = body.query
    query_filter = body.filter

    if cohort_filter is None == 0:
        raise HTTPException(status_code=400, detail="Must have the cohort_query filter")

    if query_filter is None or len(query_filter) == 0:
        raise HTTPException(status_code=400, detail="Must have the query filter")

    if query is None or len(query_filter) == 0:
        raise HTTPException(status_code=400, detail="Must have the query")

    try:
        data = await cases.cohort_query(
            gen3_graphql_client=gen3_graphql_client,
            case_index=case_index,
            cohort_item_field=cohort_item_field,
            query=query,
            cohort_filter=cohort_filter,
            filter=query_filter,
            limit=limit,
            access_token=access_token,
        )

        return JSONResponse(status_code=status.HTTP_200_OK, content=data)
    except Exception as e:
        logger.error(f"Error while processing cohort query: {e}")
        raise HTTPException(status_code=500, detail="Error with cohort query")


class TopGenesChartRequest(BaseModel):
    cohort_filters: Dict = {"and": []}
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
    if (
        (case_filters is None)
        or (case_filters.get("and") is None)
        or (len(case_filters["and"]) == 0)
    ):
        case_filters = {"and": []}

    case_filters["and"].append({"in": {"available_variation_data": ["ssm"]}})

    # gene_filters = body.gene_filters
    ssm_filters = body.ssm_filters
    if (
        (ssm_filters is None)
        or (ssm_filters.get("and") is None)
        or (len(ssm_filters["and"]) == 0)
    ):
        ssm_filters = {"and": []}

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

    print(
        "case ids",
        json.dumps({"nested": {"path": "case", "in": {"case_id": case_ids}}}),
    )

    # build a filter containing the cohort ids and merge with the other filters
    gene_filters = {
        "and": [
            {"eq": {"is_cancer_gene_census": True}}
            #  ,  {"nested": {"path": "case", "in": {"case_id": case_ids}}}
        ]
    }
    gene_data = await cases.get_item_ids(
        gen3_graphql_client=gen3_graphql_client,
        item_fields=["gene_id"],
        doc_type="Gene_gene",
        guppy_filter=gene_filters,
        access_token=access_token,
    )

    gene_index_ids = [glom(x, "gene_id") for x in glom(gene_data, f"data.Gene_gene")]

    print(
        "gene ids",
        json.dumps({"nested": {"path": "gene", "in": {"gene_id": gene_index_ids}}}),
    )

    ssm_filters["and"].append({"nested": {"path": "case", "in": {"case_id": case_ids}}})
    ssm_filters["and"].append(
        {
            "nested": {
                "path": "case",
                "nested": {
                    "path": "case.project",
                    "in": {"project_id": ["MMRF-COMMPASS"]},
                },
            }
        }
    )

    # use case id and gene filters to get a list of genes

    gene_data = await cases.get_item_ids(
        gen3_graphql_client=gen3_graphql_client,
        doc_type="SsmOccurrence_ssm_occurrence",
        item_fields=["case.case_id", "ssm.consequence.transcript.gene.gene_id"],
        guppy_filter=ssm_filters,
        access_token=access_token,
    )

    if (gene_data.get("data") is None) or (
        gene_data.get("data").get("SsmOccurrence_ssm_occurrence") is None
    ):
        return JSONResponse(
            status_code=status.HTTP_200_OK, content={"hits": [], "total": 0}
        )

    genomic_case_ids = [
        glom(x, "case.case_id")
        for x in glom(gene_data, f"data.SsmOccurrence_ssm_occurrence")
    ]
    gene_ids = [
        glom(glom(x, "ssm.consequence")[0], "transcript.gene.gene_id")
        for x in glom(gene_data, f"data.SsmOccurrence_ssm_occurrence")
    ]

    # gene_ids = []
    # for x in glom(gene_data, f"data.SsmOccurrence_ssm_occurrence"):
    #     a = glom(x, 'ssm.consequence')[0]

    # ssm_filters["and"].append({ "nested" : { "path": "occurrence", "nested" : { "path": "occurrence.case",  "in": {  "case_id": case_ids }}}})
    # ssm_gene_data = await cases.get_item_ids(
    #     gen3_graphql_client=gen3_graphql_client,
    #     doc_type="Ssm_ssm",
    #     item_field="occurrence.case.case_id",
    #     guppy_filter=ssm_filters,
    #     access_token=access_token,
    # )

    # ssm_gene_ids = [glom(x, "transcript.gene.gene_id") for x in glom(ssm_gene_data, f"data.Ssm_ssm.consequence")]

    case_set = set(case_ids)
    ssm_gene_set = set(genomic_case_ids)
    print("case_set ", len(list(case_set)))
    filtered_case_ids = case_set.intersection(ssm_gene_set)

    print("filtered case ids", len(list(filtered_case_ids)))
    print("ssm_gene_set", len(list(ssm_gene_set)))

    top_cases_query = """
   query topGeneCases($filters: JSON) {
    CaseCentric__aggregation {
        case_centric(filter: $filters) {
        _totalCount
            gene {
                symbol {
                    histogram {
                        key
                        count
                    }
                }
            }
        }
    }
}
   """

    top_genee_filters = {
        "filters": {
            "and": [
                {"in": {"case_id": list(filtered_case_ids)}},
                {"nested": {"path": "gene", "in": {"gene_id": gene_index_ids}}},
            ]
        }
    }

    chart_data = await gen3_graphql_client.execute(
        access_token=access_token, query=top_cases_query, variables=top_genee_filters
    )

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "results": chart_data,
        },
    )
