"""
Gene Expression Data Loader.

Class to load gene expression data given:
- SQLite database for gene/case metadata (gene_id -> symbol, case_id -> submitter_id)
- Binary row files for expression values
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
from numpy import typing as npt

from gen3analysis.settings import logger


class GeneExpressionDataLoaderError(Exception):
    """Base exception for data loader errors."""

    pass


class SQLiteConnectionError(GeneExpressionDataLoaderError):
    """Raised when SQLite connection fails."""

    pass


class DataFileNotFoundError(GeneExpressionDataLoaderError):
    """Raised when a required data file is not found."""

    pass


class GeneExpressionDataLoader:
    """
    Class for loading gene expression data from SQLite db and binary row files.

    Data loader methods to:
    - Load gene metadata (gene_id -> symbol mapping) into a dict
    - Load case metadata (case_id -> submitter_id mapping) into a dict
    - Load expression values for individual genes from .bin files into numpy array

    Assumes binary files follow the format: {gene_id}_{type}.bin where type can be
    either 'uqfpkms' or 'log2_uqfpkms', and each file contains a numpy float32
    array

    Attributes:
        sqlite_path: Path to the SQLite database
        data_dir: Path to the directory containing .bin files
    """

    def __init__(
        self,
        sqlite_path: str,
        data_dir: str,
    ) -> None:
        """
        Init the data loader.

        Args:
            sqlite_path: Path to the SQLite database file
            data_dir: Path to the directory containing binary expression files

        Raises:
            SQLiteConnectionError: If the SQLite database cannot be opened
            DataFileNotFoundError: If sqlite_path doesn't exist
        """
        self.sqlite_path = Path(sqlite_path)
        self.data_dir = Path(data_dir)

        if not self.sqlite_path.exists():
            raise DataFileNotFoundError(
                f"SQLite database not found: {self.sqlite_path}"
            )
        if not self.data_dir.exists():
            raise DataFileNotFoundError(f"Data directory not found: {self.data_dir}")

    def _get_connection(self) -> sqlite3.Connection:
        """
        Get a SQLite connection.

        Returns:
            SQLite connection object

        Raises:
            SQLiteConnectionError: If connection fails
        """
        try:
            return sqlite3.connect(str(self.sqlite_path))
        except sqlite3.Error as e:
            raise SQLiteConnectionError(
                f"Failed to connect to SQLite database: {e}"
            ) from e

    def load_gene_metadata(self) -> Dict[str, str]:
        """
        Load gene_id -> symbol mapping

        Returns:
            Dictionary mapping gene_id to symbol

        Raises:
            SQLiteConnectionError: If database connection or query fails
        """
        try:
            conn = self._get_connection()
            cursor = conn.execute("SELECT gene_id, symbol FROM genes ORDER BY gene_id")
            result = {row[0]: row[1] for row in cursor}
            conn.close()
            logger.debug("Loaded %d gene metadata entries", len(result))
            return result
        except sqlite3.Error as e:
            raise SQLiteConnectionError(f"Failed to load gene metadata: {e}") from e

    def load_case_metadata(self) -> Dict[str, str]:
        """
        Load case_id -> submitter_id mapping

        Returns:
            Dictionary mapping case_id to submitter_id

        Raises:
            SQLiteConnectionError: If database connection or query fails
        """
        try:
            conn = self._get_connection()
            cursor = conn.execute(
                "SELECT case_id, submitter_id FROM cases ORDER BY case_id"
            )
            result = {row[0]: row[1] for row in cursor}
            conn.close()
            logger.debug("Loaded %d case metadata entries", len(result))
            return result
        except sqlite3.Error as e:
            raise SQLiteConnectionError(f"Failed to load case metadata: {e}") from e

    def _get_expression_file_path(
        self, gene_id: str, value_type: str = "uqfpkm"
    ) -> Path:
        """Get the path to the expression values file for a gene.

        Args:
            gene_id: ENSEMBL gene ID
            value_type: "uqfpkm" or "log2_uqfpkm"

        Returns:
            Path to the binary file
        """
        suffix = "log2_uqfpkms" if value_type == "log2_uqfpkm" else "uqfpkms"
        return self.data_dir / f"{gene_id}_{suffix}.bin"

    def load_gene_expression_values(
        self, gene_id: str, value_type: str = "uqfpkm"
    ) -> npt.NDArray[np.float32]:
        """
        Load expression values for a single gene from .bin file.

        Args:
            gene_id: ENSEMBL gene ID
            value_type: "uqfpkm" for raw uqfpkm values,
                       "log2_uqfpkm" for log2(uqfpkm + 1) values

        Returns:
            numpy float32 array with one value per case

        Raises:
            DataFileNotFoundError: If the binary file doesn't exist
            ValueError: If value_type is invalid
        """
        if value_type not in ("uqfpkm", "log2_uqfpkm"):
            raise ValueError(f"Invalid value_type: {value_type}")

        file_path = self._get_expression_file_path(gene_id, value_type)

        if not file_path.exists():
            raise DataFileNotFoundError(
                f"Expression file not found for gene {gene_id}: {file_path}"
            )

        try:
            # Read the binary file as float32 array
            data = np.fromfile(str(file_path), dtype=np.float32)
            logger.debug(
                "Loaded expression values for gene %s: %d values",
                gene_id,
                len(data),
            )
            return data
        except Exception as e:
            raise GeneExpressionDataLoaderError(
                f"Failed to load expression values for gene {gene_id}: {e}"
            ) from e

    def load_multiple_gene_expression_values(
        self,
        gene_ids: List[str],
        value_type: str = "uqfpkm",
    ) -> Dict[str, npt.NDArray[np.float32]]:
        """Load expression values for multiple genes.

        TODO: evaluate on_missing functionality

        Args:
            gene_ids: List of ENSEMBL gene IDs
            value_type: "uqfpkm" or "log2_uqfpkm"
            on_missing: How to handle missing files:
                - "skip": Skip genes without files (default)
                - "zeros": Return zeros array for missing genes
                - "error": Raise exception for missing genes

        Returns:
            Dictionary mapping gene_id to numpy array of expression values

        Raises:
            DataFileNotFoundError: If on_missing="error" and a file is missing
        """
        result: Dict[str, npt.NDArray[np.float32]] = {}
        num_cases: Optional[int] = None

        for gene_id in gene_ids:
            try:
                values = self.load_gene_expression_values(gene_id, value_type)
                result[gene_id] = values
                if num_cases is None:
                    num_cases = len(values)
            except DataFileNotFoundError:
                raise

        return result

    def has_expression_file(self, gene_id: str, value_type: str = "uqfpkm") -> bool:
        """Check if expression file exists for a gene.

        Args:
            gene_id: ENSEMBL gene ID
            value_type: "uqfpkm" or "log2_uqfpkm"

        Returns:
            True if the file exists, False otherwise
        """
        file_path = self._get_expression_file_path(gene_id, value_type)
        return file_path.resolve().exists()

    def get_case_ids_ordered(self) -> List[str]:
        """Get ordered list of case IDs from the database.

        The order of case IDs corresponds to the index positions in expression
        value arrays.

        Returns:
            List of case IDs in order

        Raises:
            SQLiteConnectionError: If database query fails
        """
        try:
            conn = self._get_connection()
            cursor = conn.execute("SELECT case_id FROM cases ORDER BY case_id")
            result = [row[0] for row in cursor]
            conn.close()
            return result
        except sqlite3.Error as e:
            raise SQLiteConnectionError(f"Failed to get case IDs: {e}") from e

    def get_gene_ids_ordered(self) -> List[str]:
        """Get ordered list of gene IDs from the database.

        Returns:
            List of gene IDs in order

        Raises:
            SQLiteConnectionError: If database query fails
        """
        try:
            conn = self._get_connection()
            cursor = conn.execute("SELECT gene_id FROM genes ORDER BY gene_id")
            result = [row[0] for row in cursor]
            conn.close()
            return result
        except sqlite3.Error as e:
            raise SQLiteConnectionError(f"Failed to get gene IDs: {e}") from e
