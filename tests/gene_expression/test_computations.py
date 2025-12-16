"""Unit tests for gene expression computations."""

from typing import List

import numpy as np
import pytest

from gen3analysis.gene_expression.computations import (
    STDDEV_MARGIN_ERROR_AROUND_ZERO,
    GeneExpression,
    compute_gene_statistics,
    compute_log2_uqfpkm,
    compute_median_and_stddev,
    compute_median_centered_log2_uqfpkm,
    is_almost_zero,
    select_top_genes,
)


def test_compute_log2():
    """Verify log2(x+1) transformation for known values."""
    uqfpkm = np.array([0.0, 1.0, 3.0, 7.0], dtype=np.float32)
    result = compute_log2_uqfpkm(uqfpkm)
    assert result.dtype == np.float32
    assert result.shape == uqfpkm.shape
    expected = np.array([0.0, 1.0, 2.0, 3.0], dtype=np.float32)
    np.testing.assert_array_almost_equal(result, expected)


class TestComputeMedianCentered:
    """Tests for compute_median_centered_log2_uqfpkm."""

    def test_1d_array(self):
        """Verify 1D array."""
        values = np.array([1.0, 2.0, 3.0, 4.0, 5.0], dtype=np.float32)
        result = compute_median_centered_log2_uqfpkm(values)
        assert result.dtype == np.float32
        expected = np.array([-2.0, -1.0, 0.0, 1.0, 2.0], dtype=np.float32)
        np.testing.assert_array_almost_equal(result, expected)

        values = np.array([1.0, 2.0, 3.0, 4.0], dtype=np.float32)
        result = compute_median_centered_log2_uqfpkm(values)
        expected = np.array([-1.5, -0.5, 0.5, 1.5], dtype=np.float32)
        np.testing.assert_array_almost_equal(result, expected)

    def test_2d_array_row_wise(self):
        """Verify median centering is applied row-wise for 2D array."""
        values = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]], dtype=np.float32)
        result = compute_median_centered_log2_uqfpkm(values)
        expected = np.array([[-1.0, 0.0, 1.0], [-1.0, 0.0, 1.0]], dtype=np.float32)
        np.testing.assert_array_almost_equal(result, expected)

    def test_empty_array(self):
        """Verify empty array handling."""
        values = np.array([], dtype=np.float32)
        result = compute_median_centered_log2_uqfpkm(values)
        assert result.shape == (0,)


class TestGeneExpression:
    """GeneExpression unit tests"""

    def test_immutable(self):
        """Verify GeneExpression is immutable."""
        stats = GeneExpression(gene_id="ENSG0001", stddev=1.5, median=2.0)
        with pytest.raises(Exception):
            stats.stddev = 2.0

    def test_priority(self):
        """Verify gene expression priority"""
        high_stddev = GeneExpression(gene_id="ENSG0001", stddev=2.0, median=1.0)
        low_stddev = GeneExpression(gene_id="ENSG0002", stddev=1.0, median=1.0)
        assert low_stddev.has_lower_priority(high_stddev)
        assert not high_stddev.has_lower_priority(low_stddev)

        low_median = GeneExpression(gene_id="ENSG0001", stddev=1.5, median=1.0)
        high_median = GeneExpression(gene_id="ENSG0002", stddev=1.5, median=2.0)
        assert high_median.has_lower_priority(low_median)
        assert not low_median.has_lower_priority(high_median)

        gene_a = GeneExpression(gene_id="ENSG0001", stddev=1.5, median=2.0)
        gene_b = GeneExpression(gene_id="ENSG0002", stddev=1.5, median=2.0)
        assert gene_b.has_lower_priority(gene_a)
        assert not gene_a.has_lower_priority(gene_b)

    def test_lt_comparison(self):
        """Verify less than comparison for heapq."""
        high_priority = GeneExpression(gene_id="ENSG0001", stddev=2.0, median=1.0)
        low_priority = GeneExpression(gene_id="ENSG0002", stddev=1.0, median=1.0)
        assert low_priority < high_priority
        assert not high_priority < low_priority


class TestComputeGeneExpression:
    """Tests for compute_gene_statistics function."""

    def test_statistics(self):
        """Verify correct stddev and median."""
        values = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]], dtype=np.float32)
        gene_ids = ["ENSG0001", "ENSG0002"]
        result = compute_gene_statistics(values, gene_ids)
        assert len(result) == 2
        assert all(isinstance(s, GeneExpression) for s in result)
        assert result[0].gene_id == "ENSG0001"
        assert result[1].gene_id == "ENSG0002"
        assert abs(result[0].median - 2.0) < 0.01
        assert abs(result[1].median - 5.0) < 0.01
        assert abs(result[0].stddev - result[1].stddev) < 0.01

    def test_single_gene_1d_input(self):
        """Verify 1D input is handled correctly."""
        values = np.array([1.0, 2.0, 3.0], dtype=np.float32)
        gene_ids = ["ENSG0001"]
        result = compute_gene_statistics(values, gene_ids)
        assert len(result) == 1
        assert result[0].gene_id == "ENSG0001"

    def test_mismatched_gene_ids_raises_error(self):
        """Verify error when gene_ids count doesn't match rows."""
        values = np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32)
        gene_ids = ["ENSG0001"]
        with pytest.raises(ValueError, match="doesn't match"):
            compute_gene_statistics(values, gene_ids)


def test_almost_zero():
    """Tests almost zero function."""
    assert is_almost_zero(0.0)
    assert is_almost_zero(STDDEV_MARGIN_ERROR_AROUND_ZERO)
    assert is_almost_zero(-STDDEV_MARGIN_ERROR_AROUND_ZERO)
    assert is_almost_zero(STDDEV_MARGIN_ERROR_AROUND_ZERO / 2)
    assert is_almost_zero(-STDDEV_MARGIN_ERROR_AROUND_ZERO / 2)
    assert not is_almost_zero(STDDEV_MARGIN_ERROR_AROUND_ZERO + 0.0001)
    assert not is_almost_zero(-STDDEV_MARGIN_ERROR_AROUND_ZERO - 0.0001)
    assert not is_almost_zero(1.0)


class TestSelectTopGenes:
    """Tests for select_top_genes function."""

    def test_returns_top_n_by_stddev(self):
        """Verify top genes are selected by stddev descending."""
        stats = [
            GeneExpression(gene_id="GENE_A", stddev=3.0, median=2.0),
            GeneExpression(gene_id="GENE_B", stddev=2.0, median=1.5),
            GeneExpression(gene_id="GENE_C", stddev=4.0, median=2.5),
            GeneExpression(gene_id="GENE_D", stddev=1.0, median=3.0),
            GeneExpression(gene_id="GENE_E", stddev=2.5, median=0.5),
        ]
        result = select_top_genes(stats, selection_size=3)
        assert len(result) == 3
        assert result[0].gene_id == "GENE_C"
        assert result[1].gene_id == "GENE_A"
        assert result[2].gene_id == "GENE_B"

        result = select_top_genes(stats, selection_size=5, min_median_log2_uqfpkm=1.0)
        # GENE_E has median 0.5 < 1.0, should be excluded
        gene_ids = [g.gene_id for g in result]
        assert "GENE_E" not in gene_ids

        result = select_top_genes(stats, selection_size=0)
        assert result == []

    def test_excludes_zero_stddev_genes(self):
        """Verify genes with stddev ≈ 0 are excluded."""
        stats = [
            GeneExpression(gene_id="GENE_A", stddev=2.0, median=2.0),
            GeneExpression(gene_id="GENE_B", stddev=0.0, median=2.0),
            GeneExpression(gene_id="GENE_C", stddev=0.00001, median=2.0),
        ]
        result = select_top_genes(stats, selection_size=3)
        gene_ids = [g.gene_id for g in result]
        assert "GENE_A" in gene_ids
        assert "GENE_B" not in gene_ids
        assert "GENE_C" not in gene_ids

    def test_tiebreaker_ordering(self):
        """Verify tiebreaker ordering."""
        stats = [
            GeneExpression(gene_id="GENE_B", stddev=2.0, median=3.0),
            GeneExpression(gene_id="GENE_A", stddev=2.0, median=3.0),
            GeneExpression(gene_id="GENE_C", stddev=2.0, median=2.0),
        ]
        result = select_top_genes(stats, selection_size=3)
        assert result[0].gene_id == "GENE_C"
        assert result[1].gene_id == "GENE_A"
        assert result[2].gene_id == "GENE_B"


def test_compute_median_and_stddev():
    """Tests for compute_median_and_stddev function."""
    values = np.array([1.0, 2.0, 3.0, 4.0, 5.0], dtype=np.float32)
    stddev, median = compute_median_and_stddev(values)
    np.testing.assert_almost_equal(median, 3.0)
    expected_stddev = np.std([1.0, 2.0, 3.0, 4.0, 5.0])
    np.testing.assert_almost_equal(stddev, expected_stddev)
