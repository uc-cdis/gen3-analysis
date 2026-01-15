"""
Gene Expression API Routes.

This module provides FastAPI routes for the Gene Expression API:
- POST /gene_expression/availability - Check gene expression data availability
- POST /gene_expression/values - Get expression values for cases and genes
- POST /gene_expression/gene_selection - Select most variably expressed genes

Reference: GDC Gene Expression API
https://github.com/NCI-GDC/gdcapi/tree/develop/src/gdcapi/gene_expression
"""

from __future__ import annotations

from io import StringIO
from typing import List, Optional

from fastapi import APIRouter, Cookie, HTTPException, Request, Depends
from fastapi.responses import JSONResponse, StreamingResponse

from gen3analysis.dependencies.guppy_client import get_guppy_client
from gen3analysis.gen3.guppyQuery import GuppyGQLClient

from gen3analysis.gene_expression.computations import (
    compute_gene_statistics,
    compute_median_centered_log2_uqfpkm,
    select_top_genes,
)
from gen3analysis.gene_expression.data_store import (
    DataStoreNotInitializedError,
    GeneExpressionDataStore,
)
from gen3analysis.models.gene_expression import (
    AvailabilityRequest,
    AvailabilityResponse,
    CaseDetail,
    CaseInfo,
    CaseResourceAvailability,
    ExpressionValue,
    GeneDetail,
    GeneInfo,
    GeneResourceAvailability,
    GeneSelectionRequest,
    GeneSelectionResponse,
    GeneType,
    OutputFormat,
    SelectedGene,
    TSVUnits,
    ValuesRequest,
    ValuesResponse,
)
from gen3analysis.query_builders.cases.cases import get_item_ids
from gen3analysis.settings import logger, settings
from glom import glom

gene_expression = APIRouter()


def get_data_store() -> GeneExpressionDataStore:
    """Get the initialized gene expression data store.

    Returns:
        The initialized GeneExpressionDataStore singleton

    Raises:
        HTTPException: If gene expression is not enabled or data store not initialized
    """
    if not settings.GENE_EXPRESSION_ENABLED:
        raise HTTPException(
            status_code=503,
            detail="Gene expression API is not enabled",
        )

    data_store = GeneExpressionDataStore.get_instance()
    if not data_store.is_loaded():
        raise HTTPException(
            status_code=503,
            detail="Gene expression data store is not initialized",
        )
    return data_store


async def resolve_case_ids(
    request: Request,
    case_ids: Optional[List[str]] = None,
    case_filters: Optional[dict] = None,
    gen3_graphql_client: Optional[GuppyGQLClient] = None,
    access_token: Optional[str] = None,
) -> Optional[List[str]]:
    """
    Resolve case IDs.

    For now only case_ids is supported.

    Returns:
        List of resolved case IDs, or None if no case source is provided
    """
    if case_ids:
        return case_ids

    if case_filters:
        results = await get_item_ids(
            gen3_graphql_client,
            settings.case_centric_gql,
            ["case_id"],
            case_filters,
            limit=settings.MAX_CASES,
            access_token=access_token,
        )
        ids = list(
            set(case["case_id"] for case in results["data"][settings.case_centric_gql])
        )

        if not ids:
            logger.warning(
                "No case IDs found for filters %s",
                case_filters,
            )

        return ids

    return None


# ============================================================================
# POST /gene_expression/availability
# ============================================================================


@gene_expression.post(
    "/availability",
    response_model=AvailabilityResponse,
    summary="Check gene expression data availability",
    description="""
    Check the availability of gene expression data for cases and/or genes.

    Returns counts and details about which cases and genes have gene expression data available.
    """,
)
async def availability(
    request: Request,
    body: AvailabilityRequest,
    access_token: Optional[str] = Cookie(None),
    gen3_graphql_client: GuppyGQLClient = Depends(get_guppy_client),
) -> AvailabilityResponse:
    """
    Check gene expression data availability for cases and/or genes.

    Args:
        request: FastAPI request object
        body: Request body with case_ids and/or gene_ids to check

    Returns:
        AvailabilityResponse with counts and details for requested resources
    """
    try:
        data_store = get_data_store()
    except HTTPException:
        raise
    except DataStoreNotInitializedError as e:
        raise HTTPException(status_code=503, detail=str(e))

    available_genes = data_store.get_available_genes()
    available_cases = data_store.get_available_cases()

    # Build gene availability response
    gene_availability = None
    if body.gene_ids:
        gene_ids = sorted(body.gene_ids)
        gene_details = [
            GeneDetail(
                gene_id=gene_id,
                has_gene_expression_values=(gene_id in available_genes),
            )
            for gene_id in gene_ids
        ]
        with_count = sum(1 for d in gene_details if d.has_gene_expression_values)
        without_count = len(gene_details) - with_count

        gene_availability = GeneResourceAvailability(
            with_gene_expression_count=with_count,
            without_gene_expression_count=without_count,
            details=gene_details,
        )

    # Build case availability response
    case_availability = None

    # Resolve case IDs
    resolved_case_ids = await resolve_case_ids(
        request,
        case_ids=body.case_ids,
        case_filters=body.case_filters,
        gen3_graphql_client=gen3_graphql_client,
        access_token=access_token,
    )

    if resolved_case_ids:
        case_ids = sorted(resolved_case_ids)
        case_details = [
            CaseDetail(
                case_id=case_id,
                has_gene_expression_values=(case_id in available_cases),
            )
            for case_id in case_ids
        ]
        with_count = sum(1 for d in case_details if d.has_gene_expression_values)
        without_count = len(case_details) - with_count

        case_availability = CaseResourceAvailability(
            with_gene_expression_count=with_count,
            without_gene_expression_count=without_count,
            details=case_details,
        )

    return AvailabilityResponse(
        genes=gene_availability,
        cases=case_availability,
    )


# ============================================================================
# POST /gene_expression/values
# ============================================================================


def build_tsv_response(
    gene_ids: List[str],
    case_ids: List[str],
    expression_matrix,
    data_store: GeneExpressionDataStore,
    tsv_units: TSVUnits,
) -> str:
    """
    Build a TSV string for expression values.

    Format:
    gene_id\tcase_id_1\tcase_id_2\t...
    ENSG...\tvalue1\tvalue2\t...

    Args:
        gene_ids: List of gene IDs (rows)
        case_ids: List of case IDs (columns)
        expression_matrix: 2D numpy array of expression values
        data_store: Data store for metadata
        tsv_units: The units to use for values

    Returns:
        TSV formatted string
    """
    import numpy as np

    output = StringIO()

    # Header: gene_id followed by case_ids (or submitter_ids)
    column_headers = []
    for case_id in case_ids:
        submitter_id = data_store.get_case_submitter_id(case_id)
        column_headers.append(submitter_id)

    output.write("gene_id\t" + "\t".join(column_headers) + "\n")

    # If median_centered, compute median-centered values
    if tsv_units == TSVUnits.MEDIAN_CENTERED_LOG2_UQFPKM:
        values_matrix = compute_median_centered_log2_uqfpkm(expression_matrix)
    else:
        # For uqfpkm, we need to convert log2 back to raw values
        # We didn't load non log2 in data store
        values_matrix = np.power(2, expression_matrix) - 1

    # Data rows: gene_id followed by values
    for row_idx, gene_id in enumerate(gene_ids):
        row_values = [
            str(values_matrix[row_idx, col_idx]) for col_idx in range(len(case_ids))
        ]
        output.write(f"{gene_id}\t" + "\t".join(row_values) + "\n")

    return output.getvalue()


def build_json_response(
    gene_ids: List[str],
    case_ids: List[str],
    expression_matrix,
    data_store: GeneExpressionDataStore,
) -> ValuesResponse:
    """
    Build a JSON response for expression values.

    Args:
        gene_ids: List of gene IDs (rows)
        case_ids: List of case IDs (columns)
        expression_matrix: 2D numpy array of expression values
        data_store: Data store for metadata

    Returns:
        ValuesResponse with genes, cases, and expression values matrix
    """
    gene_expressions = compute_gene_statistics(expression_matrix, gene_ids)
    median_centered = compute_median_centered_log2_uqfpkm(expression_matrix)

    # Build gene info list
    genes_info = []
    for idx, gene_id in enumerate(gene_ids):
        symbol = data_store.get_gene_symbol(gene_id) or ""
        genes_info.append(
            GeneInfo(
                gene_id=gene_id,
                symbol=symbol,
                log2_uqfpkm_median=gene_expressions[idx].median,
                log2_uqfpkm_stddev=gene_expressions[idx].stddev,
            )
        )

    # Build case info list
    cases_info = []
    for case_id in case_ids:
        submitter_id = data_store.get_case_submitter_id(case_id)
        cases_info.append(
            CaseInfo(
                case_id=case_id,
                submitter_id=submitter_id,
            )
        )

    # Build expression values matrix [gene_idx][case_idx]
    expressed_values: List[List[ExpressionValue]] = []
    for row_idx in range(len(gene_ids)):
        row_values = []
        for col_idx in range(len(case_ids)):
            log2_val = float(expression_matrix[row_idx, col_idx])
            median_centered_val = float(median_centered[row_idx, col_idx])
            row_values.append(
                ExpressionValue(
                    log2_uqfpkm=log2_val,
                    median_centered_log2_uqfpkm=median_centered_val,
                )
            )
        expressed_values.append(row_values)

    return ValuesResponse(
        genes=genes_info,
        cases=cases_info,
        expressed_values=expressed_values,
    )


@gene_expression.post(
    "/values",
    summary="Get gene expression values",
    description="""
    Get gene expression values for the specified cases and genes.

    Returns expression values in TSV (default) or JSON format.

    For TSV format, use tsv_units to specify the units:
    - uqfpkm: Raw FPKM-UQ values (default)
    - median_centered_log2_uqfpkm: Median-centered log2(FPKM-UQ+1) values
    """,
)
async def values(
    request: Request,
    body: ValuesRequest,
):
    """
    Get gene expression values for specified cases and genes.

    Args:
        request: FastAPI request object
        body: Request body with case/gene identifiers and format options

    Returns:
        TSV or JSON response with expression values
    """
    try:
        data_store = get_data_store()
    except HTTPException:
        raise
    except DataStoreNotInitializedError as e:
        raise HTTPException(status_code=503, detail=str(e))

    resolved_case_ids = resolve_case_ids(
        request,
        case_ids=body.case_ids,
        case_filters=body.case_filters,
        cohort_id=body.cohort_id,
    )

    if not resolved_case_ids:
        raise HTTPException(
            status_code=400,
            detail="Could not resolve any case IDs from the request",
        )

    if not body.gene_ids:
        raise HTTPException(
            status_code=400,
            detail="gene_ids must be provided",
        )

    valid_gene_ids, valid_case_ids, expression_matrix = (
        data_store.get_expression_values(
            gene_ids=body.gene_ids,
            case_ids=resolved_case_ids,
        )
    )

    if not valid_gene_ids:
        raise HTTPException(
            status_code=400,
            detail="No valid genes found with expression data",
        )

    if not valid_case_ids:
        raise HTTPException(
            status_code=400,
            detail="No valid cases found with expression data",
        )

    logger.info(
        "Retrieved expression values for %d genes and %d cases",
        len(valid_gene_ids),
        len(valid_case_ids),
    )

    # Return response in requested format
    if body.format == OutputFormat.JSON:
        response = build_json_response(
            valid_gene_ids,
            valid_case_ids,
            expression_matrix,
            data_store,
        )
        return JSONResponse(content=response.model_dump())
    else:
        # TSV format (default)
        tsv_content = build_tsv_response(
            valid_gene_ids,
            valid_case_ids,
            expression_matrix,
            data_store,
            body.tsv_units,
        )
        return StreamingResponse(
            iter([tsv_content]),
            media_type="text/tab-separated-values",
            headers={"Content-Disposition": "attachment; filename=gene_expression.tsv"},
        )


# ============================================================================
# POST /gene_expression/gene_selection
# ============================================================================


@gene_expression.post(
    "/gene_selection",
    response_model=GeneSelectionResponse,
    summary="Select most variably expressed genes",
    description="""
    Select the most variably expressed genes for a collection of cases and genes.

    Selection algorithm:
    1. For each gene, compute log2(FPKM-UQ + 1) values (pre-computed)
    2. Exclude genes with stddev ≈ 0 (no variation)
    3. Exclude genes with median < min_median_log2_uqfpkm
    4. Sort remaining genes by stddev descending
    5. Return top selection_size genes

    Note: case_set_id and gene_set_id parameters are NOT supported in Gen3.
    """,
)
async def gene_selection(
    request: Request,
    body: GeneSelectionRequest,
) -> GeneSelectionResponse:
    """
    Select the most variably expressed genes for a collection of cases and collection of genes

    Args:
        request: FastAPI request object
        body: Request body with case/gene sources and selection parameters

    Returns:
        GeneSelectionResponse with selected genes sorted by stddev descending
    """
    try:
        data_store = get_data_store()
    except HTTPException:
        raise
    except DataStoreNotInitializedError as e:
        raise HTTPException(status_code=503, detail=str(e))

    resolved_case_ids = resolve_case_ids(
        request,
        case_ids=body.case_ids,
        case_filters=body.case_filters,
        cohort_id=body.cohort_id,
    )

    if not resolved_case_ids:
        raise HTTPException(
            status_code=400,
            detail="Could not resolve any case IDs from the request",
        )

    # Resolve gene IDs
    gene_ids_to_query: List[str]
    if body.gene_ids:
        gene_ids_to_query = body.gene_ids
    elif body.gene_type == GeneType.PROTEIN_CODING:
        # NOTE: Use all available genes for gene_type = "protein_coding"
        gene_ids_to_query = list(data_store.get_available_genes())
    else:
        raise HTTPException(
            status_code=400,
            detail="Either gene_ids or gene_type must be provided",
        )

    if not gene_ids_to_query:
        raise HTTPException(
            status_code=400,
            detail="No genes to query",
        )

    valid_gene_ids, valid_case_ids, expression_matrix = (
        data_store.get_expression_values(
            gene_ids=gene_ids_to_query,
            case_ids=resolved_case_ids,
        )
    )

    if not valid_gene_ids:
        return GeneSelectionResponse(genes=[])

    if not valid_case_ids:
        return GeneSelectionResponse(genes=[])

    logger.info(
        "Computing gene selection for %d genes and %d cases",
        len(valid_gene_ids),
        len(valid_case_ids),
    )

    gene_expressions = compute_gene_statistics(expression_matrix, valid_gene_ids)

    top_genes = select_top_genes(
        gene_expressions=gene_expressions,
        selection_size=body.selection_size,
        min_median_log2_uqfpkm=body.min_median_log2_uqfpkm,
    )

    # Build response
    selected_genes = []
    for stats in top_genes:
        symbol = data_store.get_gene_symbol(stats.gene_id) or ""
        selected_genes.append(
            SelectedGene(
                gene_id=stats.gene_id,
                symbol=symbol,
                log2_uqfpkm_stddev=stats.stddev,
                log2_uqfpkm_median=stats.median,
            )
        )

    logger.info(
        "Selected %d genes (requested %d)",
        len(selected_genes),
        body.selection_size,
    )

    return GeneSelectionResponse(genes=selected_genes)
