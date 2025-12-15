"""
Gene Expression Data Store.

In-memory data store with a singleton class that:
- Loads gene expression data from SQLite and binary files using data loader
- Caches the data in RAM
- Provides methods to query genes/cases and retrieve expression values

Data store is designed for a small case x gene matrix (1k x 20k)
"""

from __future__ import annotations

import threading
from typing import ClassVar, Dict, FrozenSet, List, Optional, Set, Tuple

import numpy as np
from numpy import typing as npt

from gen3analysis.gene_expression.data_loader import (
    DataFileNotFoundError,
    GeneExpressionDataLoader,
    SQLiteConnectionError,
)

from gen3analysis.settings import logger


class GeneExpressionDataStoreError(Exception):
    """Base exception for data store errors."""

    pass


class DataStoreNotInitializedError(GeneExpressionDataStoreError):
    """Raised when data store is accessed before initialization."""

    pass


class GeneExpressionDataStore:
    """
    Singleton in-memory store for gene expression data from SQLite db and binary row files.

    Uses _loaded an instance attribute (only initialized in __new__) to track if data is loaded.

    Attributes:
        _instance: Singleton instance
        _lock: Thread lock for singleton initialization
        _loaded: Whether the data store has been initialized
        _gene_metadata: Dictionary mapping gene_id -> symbol
        _case_metadata: Dictionary mapping case_id -> submitter_id
        _gene_ids: Ordered list of gene IDs
        _case_ids: Ordered list of case IDs
        _available_genes: FrozenSet of genes with expression data
        _available_cases: FrozenSet of cases with expression data
        _expression_matrix: 2D numpy array (genes x cases) of log2(uqfpkm + 1) values
        _case_id_to_index: Dictionary mapping case_id -> matrix column index
        _loaded_gene_to_row: Dictionary mapping gene_id -> matrix row index

    Usage:
        # Retrieve the singleton instance with get_instance() class method,
        # which will load the data if it has not already been initially loaded, and return the instance
        data_store = GeneExpressionDataStore.get_instance(
            sqlite_path="/path/to/db.sqlite3",
            data_dir="/path/to/data"
        )

        # Use throughout the application
        genes = data_store.get_available_genes()
        values = data_store.get_expression_values(["ENSG..."], ["case-id-..."])
    """

    _instance: ClassVar[Optional["GeneExpressionDataStore"]] = None
    _lock: threading.Lock = threading.Lock()

    def __new__(cls) -> "GeneExpressionDataStore":
        """Create singleton instance."""
        if cls._instance is None:
            with cls._lock:
                # Double-check locking
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    # make _loaded = False in the instance
                    cls._instance._loaded = False
        return cls._instance

    def __init__(self) -> None:
        """Initialize instance variables (only sets defaults)."""
        # if _loaded == true, do not initialize attributes
        if hasattr(self, "_loaded") and self._loaded:
            return

        self._gene_metadata: Dict[str, str] = {}
        self._case_metadata: Dict[str, str] = {}
        self._gene_ids: List[str] = []
        self._case_ids: List[str] = []
        self._available_genes: FrozenSet[str] = frozenset()
        self._available_cases: FrozenSet[str] = frozenset()
        self._expression_matrix: Optional[npt.NDArray[np.float32]] = None
        self._case_id_to_index: Dict[str, int] = {}
        self._loaded_gene_to_row: Dict[str, int] = {}

    @classmethod
    def get_instance(
        cls,
        sqlite_path: Optional[str] = None,
        data_dir: Optional[str] = None,
        force_reinitialize: bool = False,
    ) -> "GeneExpressionDataStore":
        """
        Get the singleton instance of this data store.
        If data not already loaded, loads the data from sqlite_path and data_dir
        Can force re-laod

        Args:
            sqlite_path: Path to SQLite database file. Required for first initialization.
            data_dir: Path to directory containing binary expression files.
                Required for first initialization.
            force_reinitialize: If True, reinitialize even if already initialized.

        Returns:
            The singleton GeneExpressionDataStore instance

        Raises:
            GeneExpressionDataStoreError: If load fails
        """
        instance = cls()

        if force_reinitialize:
            instance._loaded = False

        if not instance._loaded and sqlite_path and data_dir:
            instance._load_data(sqlite_path, data_dir)

        return instance

    @classmethod
    def reset_instance(cls) -> None:
        """Reset the singleton instance."""
        with cls._lock:
            if cls._instance is not None:
                cls._instance._loaded = False
            cls._instance = None

    def _load_data(
        self,
        sqlite_path: str,
        data_dir: str,
    ) -> None:
        """
        Load gene expression data from files.

        Args:
            sqlite_path: Path to SQLite database file
            data_dir: Path to directory containing binary expression files
        """
        try:
            loader = GeneExpressionDataLoader(sqlite_path, data_dir)

            # Load metadata
            logger.info("Loading gene expression metadata from %s", sqlite_path)
            self._gene_metadata = loader.load_gene_metadata()
            self._case_metadata = loader.load_case_metadata()

            # Get IDs
            # NOTE: There COULD be gene ids in the sqlite db, that do not have expression files
            self._gene_ids = loader.get_gene_ids_ordered()
            self._case_ids = loader.get_case_ids_ordered()

            # Store case id index mapping, needed if requests use case_ids filter
            self._case_id_to_index = {
                case_id: idx for idx, case_id in enumerate(self._case_ids)
            }

            # Find gene expression files
            genes_with_files = []
            for gene_id in self._gene_ids:
                if loader.has_expression_file(gene_id, "log2_uqfpkm"):
                    genes_with_files.append(gene_id)
                else:
                    logger.warning(
                        f"Gene {gene_id} does not have an expression file and will not be loaded"
                    )
            gene_ids_to_load = genes_with_files

            logger.info("Loading expression values for %d genes", len(gene_ids_to_load))

            # Load expression values
            # NOTE: only loading log2_uqfpkm values for now
            expression_data = loader.load_multiple_gene_expression_values(
                gene_ids_to_load, value_type="log2_uqfpkm"
            )

            # Store sets of available genes and cases
            self._available_genes = frozenset(expression_data.keys())
            self._available_cases = frozenset(self._case_ids)

            # Build the matrix
            if expression_data:
                num_genes = len(expression_data)
                num_cases = len(self._case_ids)
                self._expression_matrix = np.zeros(
                    (num_genes, num_cases), dtype=np.float32
                )

                # Create mapping from gene_id to matrix row
                loaded_gene_to_row: Dict[str, int] = {}
                for row_idx, gene_id in enumerate(expression_data.keys()):
                    loaded_gene_to_row[gene_id] = row_idx
                    self._expression_matrix[row_idx, :] = expression_data[gene_id]

                # Store the gene_id to matrix row
                self._loaded_gene_to_row = loaded_gene_to_row
            else:
                self._expression_matrix = np.zeros(
                    (0, len(self._case_ids)), dtype=np.float32
                )
                self._loaded_gene_to_row = {}

            self._loaded = True
            logger.info(
                "Gene expression data store initialized with %d genes and %d cases",
                len(self._available_genes),
                len(self._available_cases),
            )

        except (SQLiteConnectionError, DataFileNotFoundError) as e:
            raise GeneExpressionDataStoreError(
                f"Failed to load gene expression data: {e}"
            ) from e
        except Exception as e:
            raise GeneExpressionDataStoreError(
                f"Unexpected error loading gene expression data: {e}"
            ) from e

    def is_loaded(self) -> bool:
        """Check if the data store has been initialized with data."""
        return self._loaded

    def get_available_genes(self) -> FrozenSet[str]:
        """
        Get the set of gene IDs with available expression data.

        Returns:
            FrozenSet of gene IDs

        Raises:
            DataStoreNotInitializedError: If data store is not initialized
        """
        if not self._loaded:
            raise DataStoreNotInitializedError(
                "Data store must be initialized before use"
            )
        return self._available_genes

    def get_available_cases(self) -> FrozenSet[str]:
        """
        Get the set of case IDs with available expression data.

        Returns:
            FrozenSet of case IDs

        Raises:
            DataStoreNotInitializedError: If data store is not initialized
        """
        if not self._loaded:
            raise DataStoreNotInitializedError(
                "Data store must be initialized before use"
            )
        return self._available_cases

    def get_gene_symbol(self, gene_id: str) -> Optional[str]:
        """
        Get the symbol for a gene.

        Args:
            gene_id: ENSEMBL gene ID

        Returns:
            Gene symbol or None if not found
        """
        return self._gene_metadata.get(gene_id)

    def get_case_submitter_id(self, case_id: str) -> Optional[str]:
        """
        Get the submitter ID for a case.

        Args:
            case_id: Case UUID

        Returns:
            Submitter ID or None if not found
        """
        return self._case_metadata.get(case_id)

    def get_expression_values(
        self,
        gene_ids: List[str],
        case_ids: Optional[List[str]] = None,
    ) -> Tuple[List[str], List[str], npt.NDArray[np.float32]]:
        """
        Get expression values for specified genes and cases.

        Args:
            gene_ids: List of ENSEMBL gene IDs to retrieve
            case_ids: Optional list of case IDs. If None, returns all cases.

        Returns:
            Tuple of (valid_gene_ids, valid_case_ids, expression_matrix)

        Raises:
            DataStoreNotInitializedError: If data store is not initialized
        """
        if not self._loaded:
            raise DataStoreNotInitializedError(
                "Data store must be initialized before use"
            )

        valid_gene_ids = [gid for gid in gene_ids if gid in self._available_genes]

        if case_ids is None:
            valid_case_ids = self._case_ids
        else:
            valid_case_ids = [cid for cid in case_ids if cid in self._available_cases]

        # return early if either empty
        if not valid_gene_ids or not valid_case_ids:
            return (
                valid_gene_ids,
                valid_case_ids,
                np.zeros((len(valid_gene_ids), len(valid_case_ids)), dtype=np.float32),
            )

        # Build result subset matrix
        result = np.zeros((len(valid_gene_ids), len(valid_case_ids)), dtype=np.float32)

        for row_idx, gene_id in enumerate(valid_gene_ids):
            gene_row = self._loaded_gene_to_row.get(gene_id)
            if gene_row is not None:
                for col_idx, case_id in enumerate(valid_case_ids):
                    case_col = self._case_id_to_index.get(case_id)
                    if case_col is not None:
                        result[row_idx, col_idx] = self._expression_matrix[
                            gene_row, case_col
                        ]

        return valid_gene_ids, valid_case_ids, result

    # NOTE: All methods below here are only used in unit tests for now
    # Could be used for future features
    def has_gene(self, gene_id: str) -> bool:
        """
        Check if a gene has expression data available.

        Args:
            gene_id: ENSEMBL gene ID

        Returns:
            True if the gene has expression data
        """
        if not self._loaded:
            return False
        return gene_id in self._available_genes

    def has_case(self, case_id: str) -> bool:
        """
        Check if a case has expression data available.

        Args:
            case_id: Case UUID

        Returns:
            True if the case has expression data
        """
        if not self._loaded:
            return False
        return case_id in self._available_cases

    def get_all_gene_ids(self) -> List[str]:
        """
        Get all gene IDs in the database (not just those with expression data).

        Returns:
            Ordered list of all gene IDs
        """
        if not self._loaded:
            raise DataStoreNotInitializedError(
                "Data store must be initialized before use"
            )
        return self._gene_ids.copy()

    def get_all_case_ids(self) -> List[str]:
        """
        Get all case IDs in the database.

        Returns:
            Ordered list of all case IDs
        """
        if not self._loaded:
            raise DataStoreNotInitializedError(
                "Data store must be initialized before use"
            )
        return self._case_ids.copy()

    def get_gene_count(self) -> int:
        """Get the number of genes with expression data."""
        return len(self._available_genes)

    def get_case_count(self) -> int:
        """Get the number of cases."""
        return len(self._available_cases)

    def get_gene_metadata(self) -> Dict[str, str]:
        """
        Get all gene metadata (gene_id -> symbol mapping).

        Returns:
            Dictionary mapping gene_id to symbol
        """
        if not self._loaded:
            raise DataStoreNotInitializedError(
                "Data store must be initialized before use"
            )
        return self._gene_metadata.copy()

    def get_case_metadata(self) -> Dict[str, str]:
        """
        Get all case metadata (case_id -> submitter_id mapping).

        Returns:
            Dictionary mapping case_id to submitter_id
        """
        if not self._loaded:
            raise DataStoreNotInitializedError(
                "Data store must be initialized before use"
            )
        return self._case_metadata.copy()
