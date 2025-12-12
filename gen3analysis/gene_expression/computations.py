"""
Gene Expression Computations.

Core algorithms for gene expression analysis:
- Median-centered normalization: compute_median_centered_log2_uqfpkm
- Gene statistics (stddev, median): compute_gene_statistics
- Variable gene selection: select_top_genes

Reference: Based on GDC implementations in:
- https://github.com/NCI-GDC/gdcapi/blob/main/src/gdcapi/gene_expression/gene_expression_data.py
- https://github.com/NCI-GDC/gene-expression/blob/main/src/geneexpression/gene_selection.py
- https://github.com/NCI-GDC/gene-expression/blob/main/src/geneexpression/transformers.py
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass
from typing import List, Tuple

import numpy as np
from numpy import typing as npt

# Tolerance used when comparing stddev with zero in gene selection.
STDDEV_MARGIN_ERROR_AROUND_ZERO = 0.0001


@dataclass(frozen=True)
class GeneExpression:
    """
    Stats for a single gene across cases.

    NOTE: GDC uses a namedtuple in their implementation

    Attributes:
        gene_id: ENSEMBL gene ID
        stddev: Standard deviation of log2(uqfpkm + 1) values across cases
        median: Median of log2(uqfpkm + 1) values across cases
    """

    gene_id: str
    stddev: float
    median: float

    def has_lower_priority(self, other: "GeneExpression") -> bool:
        """
        Determine if this gene has lower selection priority than another.

        Priority for gene selection:
        1. Highest stddev
        2. Lowest median
        3. Lowest gene_id alphabetically

        Args:
            other: The other gene statistics to compare against

        Returns:
            True if this gene has lower priority than the other
        """
        if self.stddev == other.stddev:
            # When stddev is equal, prefer lower median, then lower gene_id
            return (self.median, self.gene_id) > (other.median, other.gene_id)
        return self.stddev < other.stddev

    def __lt__(self, other: object) -> bool:
        """Comparison for heapq (min-heap behavior)."""
        if not isinstance(other, GeneExpression):
            return NotImplemented
        return self.has_lower_priority(other)


# Value for initializing gene selection heap.
# '~' is the last ASCII char alphabetically for tie-breaker.
_NULL_GENE_EXPRESSION = GeneExpression(
    gene_id="~~~NULL~~~",
    stddev=float("-inf"),
    median=0.0,
)


def compute_log2_uqfpkm(
    uqfpkm_values: npt.NDArray[np.float32],
) -> npt.NDArray[np.float32]:
    """
    Compute log2(uqfpkm + 1) transformation.

    TODO: This is an unused util for now
    We have log2 values in *_log2_uqfpkms.bin files already

    Args:
        uqfpkm_values: Array of uqfpkm values

    Returns:
        Array of log2(uqfpkm + 1) values
    """
    return np.log2(uqfpkm_values + 1).astype(np.float32)


def compute_median_centered_log2_uqfpkm(
    log2_values: npt.NDArray[np.float32],
) -> npt.NDArray[np.float32]:
    """
    Compute median-centered log2 values per gene.

    For each gene (row), subtracts the row median from all values.
    This normalizes the expression values so that the median expression
    for each gene is 0, making cross-gene comparisons more meaningful.

    NOTE: GDC uses `np.median`

    Args:
        log2_values: 2D array of log2(uqfpkm + 1) values with shape (genes, cases)

    Returns:
        2D array of median-centered values with same shape as input.
        Uses np.nanmedian to handle NaN values correctly.
    """
    if log2_values.size == 0:
        return log2_values.copy()

    # For 1D array
    if log2_values.ndim == 1:
        median = np.nanmedian(log2_values)
        return (log2_values - median).astype(np.float32)

    # For 2D array
    medians = np.nanmedian(log2_values, axis=1, keepdims=True)
    return (log2_values - medians).astype(np.float32)


def compute_gene_statistics(
    log2_values: npt.NDArray[np.float32],
    gene_ids: List[str],
) -> List[GeneExpression]:
    """Compute stddev and median statistics for each gene.

    Args:
        log2_values: 2D array of log2(uqfpkm + 1) values with shape (genes, cases)
        gene_ids: List of gene IDs corresponding to rows of log2_values

    Returns:
        List of GeneExpression, one per gene in the same order as gene_ids

    Raises:
        ValueError: If number of gene_ids doesn't match number of rows in log2_values
    """
    if log2_values.ndim == 1:
        # Single gene case - reshape to 2D
        log2_values = log2_values.reshape(1, -1)

    if len(gene_ids) != log2_values.shape[0]:
        raise ValueError(
            f"Number of gene_ids ({len(gene_ids)}) doesn't match "
            f"number of rows in log2_values ({log2_values.shape[0]})"
        )

    if log2_values.size == 0:
        return []

    stddevs = np.nanstd(log2_values, axis=1)
    medians = np.nanmedian(log2_values, axis=1)

    return [
        GeneExpression(gene_id=gene_id, stddev=float(std), median=float(med))
        for gene_id, std, med in zip(gene_ids, stddevs, medians)
    ]


def is_almost_zero(value: float) -> bool:
    """Determines if the value can be considered a zero value. A value is considered
    zero when it falls between the constant STDDEV_MARGIN_ERROR_AROUND_ZERO and the
    negative of said constant.

    Args:
        value: The value to be tested.

    Returns:
        True if the value is between -/+STDDEV_MARGIN_ERROR_AROUND_ZERO & thus
        effectively 0.
    """
    return -STDDEV_MARGIN_ERROR_AROUND_ZERO <= value <= STDDEV_MARGIN_ERROR_AROUND_ZERO


def select_top_genes(
    gene_expressions: List[GeneExpression],
    selection_size: int,
    min_median_log2_uqfpkm: float = 1.0,
) -> List[GeneExpression]:
    """
    Select most variably expressed genes.

    Gene Selection Algorithm:
    1. For each gene, use the pre-computed log2(uqfpkm + 1) statistics
    2. Exclude genes with stddev ≈ 0
    3. Exclude genes with median < min_median_log2_uqfpkm
    4. Sort eligible genes by stddev in descending order
    5. Return top selection_size genes

    Uses a gene expression min-heap of size selection_size.

    Args:
        gene_expressions: List of GeneExpression for all genes to consider
        selection_size: Maximum number of genes to select
        min_median_log2_uqfpkm: Minimum median threshold. Genes with median
            below this value are excluded. Default is 1.0.

    Returns:
        List of top GeneExpression, sorted by stddev descending.
        May contain fewer than selection_size genes if not enough are eligible.
    """
    if selection_size <= 0:
        return []

    # Filter to eligible genes
    eligible_count = 0

    # Initialize heap
    ranked_genes = [_NULL_GENE_EXPRESSION] * selection_size

    for gene_expr in gene_expressions:
        # Exclude genes that don't meet criteria
        if gene_expr.median < min_median_log2_uqfpkm:
            continue
        if is_almost_zero(gene_expr.stddev):
            continue

        eligible_count += 1

        # If curr gene has lower priority, not added
        heapq.heappushpop(ranked_genes, gene_expr)

    # Get the top genes, limited to actual eligible count
    top_genes = heapq.nlargest(min(selection_size, eligible_count), ranked_genes)

    return top_genes


def compute_median_and_stddev(
    values: npt.NDArray[np.float32],
) -> Tuple[float, float]:
    """Compute median and standard deviation for a single gene's values.

    TODO: This is an unused util for now

    Args:
        values: 1D array of expression values for a single gene

    Returns:
        Tuple of (stddev, median) as floats
    """
    if values.size == 0:
        return (0.0, 0.0)

    stddev = float(np.nanstd(values))
    median = float(np.nanmedian(values))
    return (stddev, median)
