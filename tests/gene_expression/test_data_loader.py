"""Unit tests for the GeneExpressionDataLoader"""

import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import numpy as np
import pytest

from gen3analysis.gene_expression.data_loader import (
    GeneExpressionDataLoader,
    SQLiteConnectionError,
    DataFileNotFoundError,
)


@pytest.fixture
def temp_sqlite_db():
    """Create a temporary SQLite database with test data."""
    with tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False) as f:
        conn = sqlite3.connect(f.name)
        conn.execute("CREATE TABLE genes (gene_id TEXT PRIMARY KEY, symbol TEXT)")
        conn.execute("CREATE TABLE cases (case_id TEXT PRIMARY KEY, submitter_id TEXT)")
        # Insert test genes
        conn.execute("INSERT INTO genes VALUES ('ENSG00000000001', 'GENE1')")
        conn.execute("INSERT INTO genes VALUES ('ENSG00000000002', 'GENE2')")
        conn.execute("INSERT INTO genes VALUES ('ENSG00000000003', 'GENE3')")
        # Insert test cases
        conn.execute("INSERT INTO cases VALUES ('case-001', 'SUBM-001')")
        conn.execute("INSERT INTO cases VALUES ('case-002', 'SUBM-002')")
        conn.execute("INSERT INTO cases VALUES ('case-003', 'SUBM-003')")
        conn.commit()
        conn.close()
        yield f.name


@pytest.fixture
def temp_data_dir():
    """Create a temporary directory with test binary files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test binary files for GENE1 (ENSG00000000001)
        test_values = np.array([1.0, 2.0, 3.0], dtype=np.float32)
        test_values.tofile(Path(tmpdir) / "ENSG00000000001_uqfpkms.bin")

        log2_values = np.log2(test_values + 1).astype(np.float32)
        log2_values.tofile(Path(tmpdir) / "ENSG00000000001_log2_uqfpkms.bin")

        # Create unexpressed gene file (all zeros) for GENE2
        zeros = np.zeros(3, dtype=np.float32)
        zeros.tofile(Path(tmpdir) / "ENSG00000000002_uqfpkms.bin")
        zeros.tofile(Path(tmpdir) / "ENSG00000000002_log2_uqfpkms.bin")

        # No GENE3 binary files to test missing file handling

        yield tmpdir


@pytest.fixture
def data_loader(temp_sqlite_db, temp_data_dir):
    """Create a data loader with test data."""
    return GeneExpressionDataLoader(temp_sqlite_db, temp_data_dir)


def test_loader_init(temp_sqlite_db, temp_data_dir):
    """Verify loader initializes with valid paths."""
    loader = GeneExpressionDataLoader(temp_sqlite_db, temp_data_dir)
    assert loader.sqlite_path == Path(temp_sqlite_db)
    assert loader.data_dir == Path(temp_data_dir)

    with pytest.raises(DataFileNotFoundError) as exc_info:
        GeneExpressionDataLoader("/nonexistent/path.sqlite3", temp_data_dir)
    assert "SQLite database not found" in str(exc_info.value)

    with pytest.raises(DataFileNotFoundError) as exc_info:
        GeneExpressionDataLoader(temp_sqlite_db, "/nonexistent/data_dir")
    assert "Data directory not found" in str(exc_info.value)


def test_load_gene_metadata(data_loader):
    """Verify loading gene metadata"""
    result = data_loader.load_gene_metadata()
    assert isinstance(result, dict)
    assert len(result) == 3
    assert "ENSG00000000001" in result
    assert "ENSG00000000002" in result
    assert "ENSG00000000003" in result
    assert result["ENSG00000000001"] == "GENE1"
    assert result["ENSG00000000002"] == "GENE2"
    assert result["ENSG00000000003"] == "GENE3"


def test_load_case_metadata(data_loader):
    """Verify loading case metadata"""
    result = data_loader.load_case_metadata()
    assert isinstance(result, dict)
    assert len(result) == 3
    assert result["case-001"] == "SUBM-001"
    assert result["case-002"] == "SUBM-002"
    assert result["case-003"] == "SUBM-003"


class TestLoadGeneExpressionValues:
    """Tests for load_gene_expression_values method."""

    def test_load_uqfpkm_values(self, data_loader):
        """Verify UQFPKM values are loaded correctly."""
        values = data_loader.load_gene_expression_values("ENSG00000000001", "uqfpkm")
        assert isinstance(values, np.ndarray)
        assert values.dtype == np.float32
        np.testing.assert_array_almost_equal(values, [1.0, 2.0, 3.0])

    def test_load_log2_uqfpkm_values(self, data_loader):
        """Verify log2 UQFPKM values are loaded correctly."""
        values = data_loader.load_gene_expression_values(
            "ENSG00000000001", "log2_uqfpkm"
        )
        assert isinstance(values, np.ndarray)
        assert values.dtype == np.float32
        expected = np.log2(np.array([1.0, 2.0, 3.0]) + 1).astype(np.float32)
        np.testing.assert_array_almost_equal(values, expected)

    def test_load_correct_array_shape(self, data_loader):
        """Verify loaded array has correct shape (one value per case)."""
        values = data_loader.load_gene_expression_values("ENSG00000000001", "uqfpkm")
        assert values.shape == (3,)

    def test_load_unexpressed_gene_returns_zeros(self, data_loader):
        """Verify unexpressed gene returns zeros."""
        values = data_loader.load_gene_expression_values("ENSG00000000002", "uqfpkm")
        np.testing.assert_array_equal(values, np.zeros(3, dtype=np.float32))

    def test_load_nonexistent_gene_raises_error(self, data_loader):
        """Verify FileNotFoundError for missing gene file."""
        with pytest.raises(DataFileNotFoundError) as exc_info:
            data_loader.load_gene_expression_values("ENSG_NONEXISTENT", "uqfpkm")
        assert "Expression file not found" in str(exc_info.value)

    def test_load_invalid_value_type_raises_error(self, data_loader):
        """Verify ValueError for invalid value_type."""
        with pytest.raises(ValueError) as exc_info:
            data_loader.load_gene_expression_values("ENSG00000000001", "invalid_type")
        assert "Invalid value_type" in str(exc_info.value)


class TestHasExpressionFile:
    """Tests for has_expression_file method."""

    def test_returns_true_for_existing_file(self, data_loader):
        """Verify returns True when file exists."""
        assert data_loader.has_expression_file("ENSG00000000001", "uqfpkm")
        assert data_loader.has_expression_file("ENSG00000000001", "log2_uqfpkm")

    def test_returns_false_for_missing_file(self, data_loader):
        """Verify returns False when file doesn't exist."""
        assert not data_loader.has_expression_file("ENSG00000000003", "uqfpkm")
        assert not data_loader.has_expression_file("NONEXISTENT", "uqfpkm")


class TestLoadMultipleGeneExpressionValues:
    """Tests for load_multiple_gene_expression_values method."""

    def test_load_multiple_genes(self, data_loader):
        """Verify loading multiple genes returns dict."""
        gene_ids = ["ENSG00000000001", "ENSG00000000002"]
        result = data_loader.load_multiple_gene_expression_values(gene_ids, "uqfpkm")
        assert isinstance(result, dict)
        assert "ENSG00000000001" in result
        assert "ENSG00000000002" in result

    def test_on_missing_error(self, data_loader):
        """Verify error behavior for missing genes."""
        gene_ids = ["ENSG00000000001", "ENSG00000000003"]
        with pytest.raises(DataFileNotFoundError):
            data_loader.load_multiple_gene_expression_values(gene_ids, "uqfpkm")


class TestGetOrderedIds:
    """Tests for ordered ID retrieval methods."""

    def test_get_gene_ids_ordered(self, data_loader):
        """Verify gene IDs are returned in order."""
        gene_ids = data_loader.get_gene_ids_ordered()
        assert isinstance(gene_ids, list)
        assert len(gene_ids) == 3
        assert gene_ids == [
            "ENSG00000000001",
            "ENSG00000000002",
            "ENSG00000000003",
        ]

    def test_get_case_ids_ordered(self, data_loader):
        """Verify case IDs are returned in order."""
        case_ids = data_loader.get_case_ids_ordered()
        assert isinstance(case_ids, list)
        assert len(case_ids) == 3
        assert case_ids == ["case-001", "case-002", "case-003"]


class TestSQLiteErrorHandling:
    """Tests for SQLite error handling."""

    def test_connection_error_on_corrupted_db(self, temp_data_dir):
        """Verify error handling for corrupted database."""
        # Create a corrupted file
        with tempfile.NamedTemporaryFile(
            suffix=".sqlite3", delete=False, mode="w"
        ) as f:
            f.write("This is not a valid SQLite database")
            corrupted_path = f.name

        loader = GeneExpressionDataLoader(corrupted_path, temp_data_dir)

        with pytest.raises(SQLiteConnectionError):
            loader.load_gene_metadata()

    def test_missing_table_error(self, temp_data_dir):
        """Verify error when table doesn't exist."""
        with tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False) as f:
            # Create empty database without tables
            conn = sqlite3.connect(f.name)
            conn.close()
            empty_db_path = f.name

        loader = GeneExpressionDataLoader(empty_db_path, temp_data_dir)

        with pytest.raises(SQLiteConnectionError):
            loader.load_gene_metadata()
