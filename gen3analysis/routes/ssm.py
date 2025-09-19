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

ssm = APIRouter()


TOP_SSMS_QUERY = """
query topSSM($filters: JSON, $geneFilters: JSON, $numGenes: Int) {
    CaseCentric__aggregation {
        case_centric(filter: $filters) {
            _totalCount
        }
    }
    Ssm_ssm (filter: $geneFilters, first: $numGenes) {
        ssm_id
        cosmic_id
        gene_aa_change
        genomic_dna_change
        mutation_subtype
        mutation_type
    }
}
"""


class SSMTableRequest(BaseModel):
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
    size: int = 10000


@ssm.post(
    path="/ssm_table",
    dependencies=[Depends(get_guppy_client)],
    status_code=status.HTTP_200_OK,
    description="Performs a cohort query and return the query for all items matching the ids.",
    summary="Get SSM table",
)
async def ssm_table(
    body: SSMTableRequest,
    access_token: Optional[str] = Cookie(None),
    gen3_graphql_client: GuppyGQLClient = Depends(get_guppy_client),
) -> JSONResponse:

    case_filters = body.cohort_filters
    if (case_filters is None) or (case_filters.get("and") is None):
        case_filters = {"and": []}

    case_filters["and"].append({"in": {"available_variation_data": ["ssm"]}})

    ssm_filters = body.ssm_filters
    if (ssm_filters is None) or (ssm_filters.get("and") is None):
        ssm_filters = {"and": []}

    gene_filters = body.gene_filters
    if (gene_filters is None) or (gene_filters.get("and") is None):
        gene_filters = {"and": []}

    data = await cases.get_multiple_item_ids(
        gen3_graphql_client=gen3_graphql_client,
        access_token=access_token,
        queryParameters={
            "case": cases.IndexQueryParameters(
                filter=case_filters,
                index="CaseCentric_case_centric",
                fields=["case_id"],
            ),
            "gene": cases.IndexQueryParameters(
                filter=gene_filters, index="Gene_gene", fields=["gene_id"]
            ),
            "ssm": cases.IndexQueryParameters(
                filter=ssm_filters,
                index="Ssm_ssm",
                fields=[
                    "occurrence.case.case_id",
                    "consequence.transcript.gene.gene_id",
                ],
            ),
        },
        limit=body.size,
    )

    # extract the case ids for the cohorts
    case_ids = set()
    for x in glom(data, f"data.case"):
        case_ids.add(glom(x, "case_id"))
    # extract the gene ids for the ssm
    gene_ids = set()
    for x in glom(data, f"data.ssm"):
        gene_ids.add(glom(x["consequence"][0], "transcript.gene.gene_id"))
    # extract the case ids for the ssm
    case_ids_ssm = set()
    for x in glom(data, f"data.ssm"):
        for occurrence in x["occurrence"]:
            case_ids_ssm.add(glom(occurrence, "case.case_id"))
    # extract the gene ids for the ssm

    case_set = set(case_ids)

    ssm_counts = defaultdict(set)
    for x in glom(data, f"data.ssm"):
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
    gene_index_set = set(gene_ids)

    for x in glom(data, f"data.ssm"):
        gene_id = glom(x["consequence"][0], "transcript.gene.gene_id")
        if gene_id in gene_index_set:
            genomic_gene_ids.add(gene_id)

        for occurrence in x["occurrence"]:
            case_id = glom(occurrence, "case.case_id")
            if case_id in case_set:
                filtered_case_ids.add(case_id)

    filter_gene_ids = list(genomic_gene_ids)

    top_gene_filters = {
        "filters": {
            "and": [
                {"nested": {"path": "gene", "in": {"gene_id": filter_gene_ids}}},
                {"in": {"case_id": list(filtered_case_ids)}},
            ]
        },
        "geneFilters": {
            "nested": {
                "path": "consequence.transcript.gene",
                "in": {"gene_id": filter_gene_ids},
            }
        },
        "numGenes": len(filter_gene_ids),
    }

    chart_data = await gen3_graphql_client.execute(
        access_token=access_token, query=TOP_SSMS_QUERY, variables=top_gene_filters
    )

    caseCount = glom(
        chart_data, "data.CaseCentric__aggregation.case_centric._totalCount"
    )

    ssm_metadata = {}
    for gene in glom(chart_data, "data.Ssm_ssm"):
        ssm_metadata[glom(gene, "ssm_id")] = gene

    ordered_gene_metadata = []
    for gene_key in ssm_counts.keys():
        if gene_key in genomic_gene_ids:
            ordered_gene_metadata.append(
                {
                    **ssm_metadata.get(gene_key),
                    **{
                        "numCases": len(list(ssm_counts[gene_key])),
                    },
                }
            )
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"ssm": ordered_gene_metadata},
    )
