"""
Gene Expression API Pydantic models.

This defines models for the Gene Expression API endpoints:
- POST /gene_expression/availability
- POST /gene_expression/values
- POST /gene_expression/gene_selection

Reference: GDC API gene expression implementation
Models generally follow the GDC Gene Expression API spec

NOTE: case_set_id and gene_set_id parameters not supported
"""

from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, field_validator, model_validator


class TSVUnits(str, Enum):
    """
    Units for gene expression values in TSV output format.

    Attributes:
        UQFPKM: Raw FPKM-UQ values (default)
        MEDIAN_CENTERED_LOG2_UQFPKM: Median-centered log2(FPKM-UQ+1) values
    """

    UQFPKM = "uqfpkm"
    MEDIAN_CENTERED_LOG2_UQFPKM = "median_centered_log2_uqfpkm"


class OutputFormat(str, Enum):
    """
    Output format for gene expression values.

    Attributes:
        TSV: Tab-separated values (default)
        JSON: JSON format
    """

    TSV = "tsv"
    JSON = "json"


class GeneType(str, Enum):
    """Type of genes to include in queries.

    Attributes:
        PROTEIN_CODING: All protein-coding genes
    """

    PROTEIN_CODING = "protein_coding"


class GeneDetail(BaseModel):
    """
    Detail about a single gene expression data availability.

    Attributes:
        gene_id: ENSEMBL gene ID
        has_gene_expression_values: Whether expression data exists for this gene
    """

    gene_id: str = Field(..., description="ENSEMBL gene ID")
    has_gene_expression_values: bool = Field(
        ..., description="Whether expression data exists for this gene"
    )


class CaseDetail(BaseModel):
    """
    Detail about a single case expression data availability.

    Attributes:
        case_id: ID of the case
        has_gene_expression_values: Whether expression data exists for this case
    """

    case_id: str = Field(..., description="UUID of the case")
    has_gene_expression_values: bool = Field(
        ..., description="Whether expression data exists for this case"
    )


class GeneResourceAvailability(BaseModel):
    """
    Availability summary and details for genes.

    Attributes:
        with_gene_expression_count: Count of genes with expression data
        without_gene_expression_count: Count of genes without expression data
        details: List of individual gene availability details
    """

    with_gene_expression_count: int = Field(
        ..., ge=0, description="Count of genes with expression data"
    )
    without_gene_expression_count: int = Field(
        ..., ge=0, description="Count of genes without expression data"
    )
    details: List[GeneDetail] = Field(
        default_factory=list, description="Individual gene availability details"
    )


class CaseResourceAvailability(BaseModel):
    """
    Availability summary and details for cases.

    Attributes:
        with_gene_expression_count: Count of cases with expression data
        without_gene_expression_count: Count of cases without expression data
        details: List of individual case availability details
    """

    with_gene_expression_count: int = Field(
        ..., ge=0, description="Count of cases with expression data"
    )
    without_gene_expression_count: int = Field(
        ..., ge=0, description="Count of cases without expression data"
    )
    details: List[CaseDetail] = Field(
        default_factory=list, description="Individual case availability details"
    )


class SelectedGene(BaseModel):
    """
    A gene selected by the gene selection algorithm.

    Attributes:
        gene_id: ENSEMBL gene ID
        symbol: Gene symbol
        log2_uqfpkm_stddev: Standard deviation of log2(uqfpkm+1) across cases
        log2_uqfpkm_median: Median of log2(uqfpkm+1) across cases
    """

    gene_id: str = Field(..., description="ENSEMBL gene ID")
    symbol: str = Field(..., description="Gene symbol")
    log2_uqfpkm_stddev: float = Field(
        ..., description="Standard deviation of log2(uqfpkm+1) across cases"
    )
    log2_uqfpkm_median: float = Field(
        ..., description="Median of log2(uqfpkm+1) across cases"
    )


class AvailabilityRequest(BaseModel):
    """
    Request body for POST /gene_expression/availability.

    At least one of case_ids, gene_ids, case_filters, or cohort_id must be provided.

    NOTE: case_set_id and gene_set_id are NOT supported.

    Attributes:
        case_ids: List of case UUIDs to check availability for
        gene_ids: List of ENSEMBL gene IDs to check availability for
        case_filters: Filter object for querying cases (GDC filter format)
        cohort_id: Identifier for a saved cohort
    """

    case_ids: Optional[List[str]] = Field(
        default=None, description="A list of IDs for cases", min_length=1
    )
    gene_ids: Optional[List[str]] = Field(
        default=None, description="A list of human ENSEMBL gene IDs", min_length=1
    )
    case_filters: Optional[Dict[str, Any]] = Field(
        default=None, description="A filter for cases"
    )
    cohort_id: Optional[str] = Field(
        default=None, description="An identifier for a cohort", min_length=1
    )

    @model_validator(mode="after")
    def validate_at_least_one_field(self) -> "AvailabilityRequest":
        """Ensure at least one query parameter is provided."""
        if not any([self.case_ids, self.gene_ids, self.case_filters, self.cohort_id]):
            raise ValueError(
                "At least one of case_ids, gene_ids, case_filters, or cohort_id "
                "must be provided"
            )
        return self


class AvailabilityResponse(BaseModel):
    """
    Response body for POST /gene_expression/availability.

    Attributes:
        genes: Availability information for genes (if gene_ids were requested)
        cases: Availability information for cases (if case_ids were requested)
    """

    genes: Optional[GeneResourceAvailability] = Field(
        default=None, description="Gene availability information"
    )
    cases: Optional[CaseResourceAvailability] = Field(
        default=None, description="Case availability information"
    )


class ValuesRequest(BaseModel):
    """
    Request body for POST /gene_expression/values.

    Must provide at least one way to specify cases (case_ids, case_filters, or cohort_id)
    and at least one way to specify genes (gene_ids).

    Note: case_set_id and gene_set_id are NOT supported in Gen3.

    Attributes:
        case_ids: List of case UUIDs
        gene_ids: List of ENSEMBL gene IDs
        case_filters: Filter object for querying cases
        cohort_id: Identifier for a saved cohort
        format: Output format (tsv or json), defaults to tsv
        tsv_units: Units for expression values if format=tsv
    """

    case_ids: Optional[List[str]] = Field(
        default=None, description="A list of UUIDs for cases", min_length=1
    )
    gene_ids: Optional[List[str]] = Field(
        default=None, description="A list of human ENSEMBL gene IDs", min_length=1
    )
    case_filters: Optional[Dict[str, Any]] = Field(
        default=None, description="A filter for GDC cases"
    )
    cohort_id: Optional[str] = Field(
        default=None, description="An identifier for a saved cohort", min_length=1
    )
    format: OutputFormat = Field(
        default=OutputFormat.TSV, description="The data format of the response"
    )
    tsv_units: TSVUnits = Field(
        default=TSVUnits.UQFPKM,
        description="The units of gene expression data if format=tsv",
    )

    @model_validator(mode="after")
    def validate_required_fields(self) -> "ValuesRequest":
        """Ensure required case and gene identifiers are provided."""
        has_cases = any([self.case_ids, self.case_filters, self.cohort_id])
        has_genes = self.gene_ids is not None

        if not has_cases:
            raise ValueError(
                "At least one of case_ids, case_filters, or cohort_id must be provided"
            )
        if not has_genes:
            raise ValueError("gene_ids must be provided")

        return self


class ExpressionValue(BaseModel):
    """
    Expression values for a single gene-case combo.

    Attributes:
        log2_uqfpkm: log2(uqfpkm + 1) value
        median_centered_log2_uqfpkm: Median-centered log2(uqfpkm+1) value
    """

    log2_uqfpkm: float = Field(..., description="log2(uqfpkm + 1) value")
    median_centered_log2_uqfpkm: float = Field(
        ..., description="Median-centered log2(uqfpkm+1) value"
    )


class GeneInfo(BaseModel):
    """
    Gene information in values response.

    Attributes:
        gene_id: ENSEMBL gene ID
        symbol: Gene symbol
        log2_uqfpkm_median: Median of log2(uqfpkm+1) across cases
        log2_uqfpkm_stddev: Standard deviation of log2(uqfpkm+1) across cases
    """

    gene_id: str = Field(..., description="ENSEMBL gene ID")
    symbol: str = Field(..., description="Gene symbol")
    log2_uqfpkm_median: float = Field(
        ..., description="Median of log2(uqfpkm+1) across cases"
    )
    log2_uqfpkm_stddev: float = Field(
        ..., description="Standard deviation of log2(uqfpkm+1) across cases"
    )


class CaseInfo(BaseModel):
    """
    Case information in values response.

    Attributes:
        case_id: ID of the case
        submitter_id: Submitter ID of the case
    """

    case_id: str = Field(..., description="ID of the case")
    submitter_id: Optional[str] = Field(
        default=None, description="Submitter ID of the case"
    )


class ValuesResponse(BaseModel):
    """
    Response body for POST /gene_expression/values (JSON format).

    Attributes:
        genes: List of gene information
        cases: List of case information
        expressed_values: matrix of expression values [gene_idx][case_idx]
    """

    genes: List[GeneInfo] = Field(..., description="Gene information list")
    cases: List[CaseInfo] = Field(..., description="Case information list")
    expressed_values: List[List[ExpressionValue]] = Field(
        ..., description="2D matrix of expression values [gene_idx][case_idx]"
    )


class GeneSelectionRequest(BaseModel):
    """
    Request body for POST /gene_expression/gene_selection.

    Must provide exactly one way to specify cases and exactly one way to specify genes.

    Note: case_set_id and gene_set_id are NOT supported in Gen3.

    Attributes:
        case_ids: List of case UUIDs
        case_filters: Filter object for querying cases
        cohort_id: Identifier for a saved cohort
        gene_ids: List of ENSEMBL gene IDs
        gene_type: Type of genes (e.g., "protein_coding" for all protein-coding genes)
        selection_size: Maximum number of genes to select (required, >= 1)
        min_median_log2_uqfpkm: Minimum median log2(FPKM-UQ+1) threshold for eligibility
    """

    # Case specification (exactly one required)
    case_ids: Optional[List[str]] = Field(
        default=None, description="A list of UUIDs for cases", min_length=1
    )
    case_filters: Optional[Dict[str, Any]] = Field(
        default=None, description="A filter for GDC cases"
    )
    cohort_id: Optional[str] = Field(
        default=None, description="An identifier for a saved cohort", min_length=1
    )

    # Gene specification (exactly one required)
    gene_ids: Optional[List[str]] = Field(
        default=None, description="A list of human ENSEMBL gene IDs", min_length=1
    )
    gene_type: Optional[GeneType] = Field(
        default=None, description="The type of genes (e.g., protein_coding)"
    )

    # Selection parameters
    selection_size: int = Field(
        ..., ge=1, description="The maximum number of genes to select"
    )
    min_median_log2_uqfpkm: float = Field(
        default=1.0,
        description="The inclusive minimum value for the median log2(FPKM-UQ+1)",
    )

    @model_validator(mode="after")
    def validate_exactly_one_of_each(self) -> "GeneSelectionRequest":
        """Ensure exactly one case source and exactly one gene source."""
        case_sources = [self.case_ids, self.case_filters, self.cohort_id]
        case_count = sum(1 for s in case_sources if s is not None)

        gene_sources = [self.gene_ids, self.gene_type]
        gene_count = sum(1 for s in gene_sources if s is not None)

        if case_count != 1:
            raise ValueError(
                "Exactly one of case_ids, case_filters, or cohort_id must be provided"
            )
        if gene_count != 1:
            raise ValueError("Exactly one of gene_ids or gene_type must be provided")

        return self


class GeneSelectionResponse(BaseModel):
    """
    Response body for POST /gene_expression/gene_selection.

    Attributes:
        genes: List of selected genes, sorted by stddev descending
    """

    genes: List[SelectedGene] = Field(
        ..., description="Selected genes sorted by standard deviation descending"
    )
