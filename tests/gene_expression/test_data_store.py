"""Unit tests for the GeneExpressionDataStore class."""

import tempfile
import sqlite3
from pathlib import Path

import numpy as np
import pytest

from gen3analysis.gene_expression.data_store import (
    GeneExpressionDataStore,
    GeneExpressionDataStoreError,
    DataStoreNotInitializedError,
)


@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset singleton instance before each test."""
    GeneExpressionDataStore.reset_instance()
    yield
    GeneExpressionDataStore.reset_instance()


@pytest.fixture
def temp_sqlite_db():
    """Create a temporary SQLite database with test data."""
    with tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False) as f:
        conn = sqlite3.connect(f.name)
        conn.execute("CREATE TABLE genes (gene_id TEXT PRIMARY KEY, symbol TEXT)")
        conn.execute("CREATE TABLE cases (case_id TEXT PRIMARY KEY, submitter_id TEXT)")

        conn.execute("INSERT INTO genes VALUES ('ENSG00000000001', 'GENE1')")
        conn.execute("INSERT INTO genes VALUES ('ENSG00000000002', 'GENE2')")
        conn.execute("INSERT INTO genes VALUES ('ENSG00000000003', 'GENE3')")

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
        # GENE1 log2 + 1 values
        log2_values_1 = np.array([1.0, 1.5849625, 2.0], dtype=np.float32)
        log2_values_1.tofile(Path(tmpdir) / "ENSG00000000001_log2_uqfpkms.bin")

        # Create unexpressed gene file (all zeros) for GENE2
        zeros = np.zeros(3, dtype=np.float32)
        zeros.tofile(Path(tmpdir) / "ENSG00000000002_log2_uqfpkms.bin")

        # No GENE3 binary files to test missing file handling

        yield tmpdir


@pytest.fixture
def initialized_store(temp_sqlite_db, temp_data_dir):
    """Return data store with loaded test data."""
    return GeneExpressionDataStore.get_instance(
        sqlite_path=temp_sqlite_db,
        data_dir=temp_data_dir,
    )


class TestSingletonPattern:
    """Tests for singleton pattern implementation."""

    def test_get_instance_returns_same_instance(self, temp_sqlite_db, temp_data_dir):
        """Verify singleton returns same instance."""
        store1 = GeneExpressionDataStore.get_instance(
            sqlite_path=temp_sqlite_db,
            data_dir=temp_data_dir,
        )
        store2 = GeneExpressionDataStore.get_instance()
        assert store1 is store2

    def test_direct_instantiation_returns_same_instance(self):
        """Verify direct instantiation also returns singleton."""
        store1 = GeneExpressionDataStore()
        store2 = GeneExpressionDataStore()
        assert store1 is store2

    def test_reset_instance_clears_singleton(self, temp_sqlite_db, temp_data_dir):
        """Verify reset_instance clears the singleton."""
        store1 = GeneExpressionDataStore.get_instance(
            sqlite_path=temp_sqlite_db,
            data_dir=temp_data_dir,
        )
        assert store1.is_loaded()

        GeneExpressionDataStore.reset_instance()

        store2 = GeneExpressionDataStore.get_instance()
        assert not store2.is_loaded()

    def test_force_reinitialize(self, temp_sqlite_db, temp_data_dir):
        """Verify force_reinitialize reloads data."""
        store = GeneExpressionDataStore.get_instance(
            sqlite_path=temp_sqlite_db,
            data_dir=temp_data_dir,
        )
        gene_count_before = store.get_gene_count()

        store = GeneExpressionDataStore.get_instance(
            sqlite_path=temp_sqlite_db,
            data_dir=temp_data_dir,
            force_reinitialize=True,
        )
        gene_count_after = store.get_gene_count()

        assert gene_count_before == gene_count_after


class TestInitialization:
    """Tests for data store initialization."""

    def test_is_loaded_false_before_init(self):
        """Verify is_loaded returns False before passing sqlite and data."""
        store = GeneExpressionDataStore.get_instance()
        assert not store.is_loaded()

    def test_is_loaded_true_after_init(self, temp_sqlite_db, temp_data_dir):
        """Verify is_loaded returns True after initialization."""
        store = GeneExpressionDataStore.get_instance(
            sqlite_path=temp_sqlite_db,
            data_dir=temp_data_dir,
        )
        assert store.is_loaded()

    def test_init_with_invalid_sqlite_path_raises_error(self, temp_data_dir):
        """Verify error when SQLite path doesn't exist."""
        with pytest.raises(GeneExpressionDataStoreError):
            GeneExpressionDataStore.get_instance(
                sqlite_path="/nonexistent/path.sqlite3",
                data_dir=temp_data_dir,
            )

    def test_init_with_invalid_data_dir_raises_error(self, temp_sqlite_db):
        """Verify error when data directory doesn't exist."""
        with pytest.raises(GeneExpressionDataStoreError):
            GeneExpressionDataStore.get_instance(
                sqlite_path=temp_sqlite_db,
                data_dir="/nonexistent/data_dir",
            )


class TestNotInitializedErrors:
    """Tests for operations on uninitialized data store."""

    def test_get_available_genes_raises_error(self):
        """Verify get_available_genes raises error before init."""
        store = GeneExpressionDataStore.get_instance()
        with pytest.raises(DataStoreNotInitializedError):
            store.get_available_genes()

    def test_get_available_cases_raises_error(self):
        """Verify get_available_cases raises error before init."""
        store = GeneExpressionDataStore.get_instance()
        with pytest.raises(DataStoreNotInitializedError):
            store.get_available_cases()

    def test_get_expression_values_raises_error(self):
        """Verify get_expression_values raises error before init."""
        store = GeneExpressionDataStore.get_instance()
        with pytest.raises(DataStoreNotInitializedError):
            store.get_expression_values(["ENSG00000000001"])

    def test_get_all_gene_ids_raises_error(self):
        """Verify get_all_gene_ids raises error before init."""
        store = GeneExpressionDataStore.get_instance()
        with pytest.raises(DataStoreNotInitializedError):
            store.get_all_gene_ids()

    def test_get_all_case_ids_raises_error(self):
        """Verify get_all_case_ids raises error before init."""
        store = GeneExpressionDataStore.get_instance()
        with pytest.raises(DataStoreNotInitializedError):
            store.get_all_case_ids()


class TestAvailableGenesAndCases:
    """Tests for gene and case availability methods."""

    def test_get_available_genes_only_includes_loaded_genes(self, initialized_store):
        """Verify only genes with expression files are included."""
        result = initialized_store.get_available_genes()
        assert isinstance(result, frozenset)
        assert "ENSG00000000001" in result
        assert "ENSG00000000002" in result
        # GENE3 has no file, should not be included
        assert "ENSG00000000003" not in result

    def test_get_available_cases_includes_all_cases(self, initialized_store):
        """Verify all cases from database are included."""
        result = initialized_store.get_available_cases()
        assert isinstance(result, frozenset)
        assert "case-001" in result
        assert "case-002" in result
        assert "case-003" in result


class TestMetadataLookup:
    """Tests for metadata lookup methods."""

    def test_get_gene_symbol_returns_symbol(self, initialized_store):
        """Verify gene symbol lookup works."""
        assert initialized_store.get_gene_symbol("ENSG00000000001") == "GENE1"
        assert initialized_store.get_gene_symbol("ENSG00000000002") == "GENE2"
        assert initialized_store.get_gene_symbol("NONEXISTENT") is None

    def test_get_case_submitter_id_returns_id(self, initialized_store):
        """Verify case submitter ID lookup works."""
        assert initialized_store.get_case_submitter_id("case-001") == "SUBM-001"
        assert initialized_store.get_case_submitter_id("case-002") == "SUBM-002"
        assert initialized_store.get_case_submitter_id("nonexistent") is None


class TestExpressionValues:
    """Tests for expression value retrieval."""

    def test_get_expression_values_returns_valid_gene_ids(self, initialized_store):
        """Verify only valid gene IDs are returned."""
        result = initialized_store.get_expression_values(["ENSG00000000001"])
        assert isinstance(result, tuple)
        assert len(result) == 3
        gene_ids, _, matrix = initialized_store.get_expression_values(
            ["ENSG00000000001", "NONEXISTENT"]
        )
        assert isinstance(matrix, np.ndarray)
        assert matrix.dtype == np.float32
        assert gene_ids == ["ENSG00000000001"]

    def test_get_expression_values_correct_shape(self, initialized_store):
        """Verify expression matrix has correct shape."""
        gene_ids, case_ids, matrix = initialized_store.get_expression_values(
            ["ENSG00000000001", "ENSG00000000002"]
        )
        assert matrix.shape == (len(gene_ids), len(case_ids))

    def test_get_expression_values_correct_values(self, initialized_store):
        """Verify expression values are correct."""
        _, _, matrix = initialized_store.get_expression_values(["ENSG00000000001"])
        # GENE1 values: [1.0, 1.58, 2.0]
        np.testing.assert_array_almost_equal(
            matrix[0, :], [1.0, 1.5849625, 2.0], decimal=3
        )

    def test_get_expression_values_subset_of_cases(self, initialized_store):
        """Verify filtering by case_ids works."""
        gene_ids, case_ids, matrix = initialized_store.get_expression_values(
            ["ENSG00000000001"],
            case_ids=["case-001", "case-003"],
        )
        assert len(case_ids) == 2
        assert matrix.shape == (1, 2)

    def test_get_expression_values_unknown_gene_excluded(self, initialized_store):
        """Verify unknown genes are excluded from results."""
        gene_ids, _, matrix = initialized_store.get_expression_values(
            ["NONEXISTENT", "ENSG00000000001"]
        )
        assert len(gene_ids) == 1
        assert gene_ids[0] == "ENSG00000000001"

    def test_get_expression_values_unknown_case_excluded(self, initialized_store):
        """Verify unknown cases are excluded from results."""
        _, case_ids, matrix = initialized_store.get_expression_values(
            ["ENSG00000000001"],
            case_ids=["case-001", "nonexistent-case"],
        )
        assert len(case_ids) == 1
        assert case_ids[0] == "case-001"

    def test_get_expression_values_empty_genes(self, initialized_store):
        """Verify empty result for no valid genes."""
        gene_ids, case_ids, matrix = initialized_store.get_expression_values(
            ["NONEXISTENT"]
        )
        assert gene_ids == []
        assert matrix.shape == (0, len(case_ids))

    def test_get_expression_values_empty_cases(self, initialized_store):
        """Verify empty result for no valid cases."""
        gene_ids, case_ids, matrix = initialized_store.get_expression_values(
            ["ENSG00000000001"],
            case_ids=["nonexistent"],
        )
        assert case_ids == []
        assert matrix.shape == (1, 0)


class TestAllIdsRetrieval:
    """Tests for retrieving all IDs."""

    def test_get_all_gene_ids_includes_all_from_db(self, initialized_store):
        """Verify all genes from database are returned (not just loaded ones)."""
        result = initialized_store.get_all_gene_ids()
        assert isinstance(result, list)
        assert len(result) == 3
        assert "ENSG00000000001" in result
        assert "ENSG00000000002" in result
        assert "ENSG00000000003" in result

    def test_get_all_case_ids_includes_all(self, initialized_store):
        """Verify all cases are returned."""
        result = initialized_store.get_all_case_ids()
        assert isinstance(result, list)
        assert len(result) == 3


class TestMetadataRetrieval:
    """Tests for retrieving metadata."""

    def test_get_gene_metadata(self, initialized_store):
        """Verify gene metadata is returned as dict."""
        result = initialized_store.get_gene_metadata()
        assert isinstance(result, dict)
        assert len(result) == 3
        result1 = initialized_store.get_gene_metadata()
        result2 = initialized_store.get_gene_metadata()
        assert result1 is not result2

    def test_get_case_metadata(self, initialized_store):
        """Verify case metadata is returned as dict."""
        result = initialized_store.get_case_metadata()
        assert isinstance(result, dict)
        assert len(result) == 3
        result1 = initialized_store.get_case_metadata()
        result2 = initialized_store.get_case_metadata()
        assert result1 is not result2
