from collections.abc import Iterable
from contextlib import closing
from io import StringIO
from typing import NamedTuple, Protocol

from defusedcsv import csv


def build_tsv_header(headers: list[str]) -> str:
    with closing(StringIO()) as output:
        writer = csv.DictWriter(
            output, fieldnames=headers, dialect=csv.excel_tab, lineterminator="\n"
        )
        writer.writeheader()
        return output.getvalue()


def _set_default_values(headers: list[str], data: dict) -> dict:
    """If a value does not exist, default it to double-dashes.

    A value is considered to not exist for any of the following:
    - The key does not exist in the dict.
    - The value is None.
    """
    data_with_defaults = data.copy()
    for header in headers:
        if data_with_defaults.get(header) is None:
            data_with_defaults[header] = "--"
    return data_with_defaults


def _build_tsv_row(headers: list[str], data: dict) -> str:
    with closing(StringIO()) as output:
        writer = csv.DictWriter(
            output,
            fieldnames=headers,
            dialect=csv.excel_tab,
            lineterminator="\n",
            extrasaction="ignore",
        )
        writer.writerow(_set_default_values(headers, data))
        return output.getvalue()


def build_tsv_lines(
    headers: list[str], entries: Iterable[dict], include_headers: bool = True
) -> list[str]:
    """Build a tsv file for the given data.

    Args:
        headers: a list of headers
        entries: list of dictionaries. Each dictionary contains keys that match header names and the associated values
        include_headers: include header as first line of returned TSV

    Returns:
        A single string containing the tsv representation of the data.
    """
    rows: list[str] = []
    if include_headers is True:
        rows.extend([build_tsv_header(headers)])

    rows.extend([_build_tsv_row(headers, entry) for entry in entries])

    return rows


def trim_nested_keys(key: str) -> str:
    """Formats keys for use in the headers and dictionary lookups to include a namespace.

    Examples:
        "diagnosis.pathology_details.days_to_pathology_detail" -> "pathology_details.days_to_pathology_detail"
        "diagnosis.cancer_detection_method" -> "diagnosis.cancer_detection_method"
        "submitter_id" -> "submitter_id"

    Args:
        key (str): the key to trim

    Returns:
        str: Returns a key that has one '.' separator maximum
    """
    return ".".join(key.split(".")[-2:])


def lines_to_bytes(lines: list[str], encoding="utf-8") -> list[bytes]:
    return [bytes(line, encoding) for line in lines]


class TsvBuilder(Protocol):
    """Interface for functions creating TSV data."""

    def __call__(self, clinical_data: list[dict]) -> list[bytes]: ...


class TsvHeaderBuilder(Protocol):
    """Interface for functions creating TSV header list."""

    def __call__(self) -> list[str]: ...


class FileBuilder(NamedTuple):
    """Class to store logic for building specific TSV files."""

    name: str
    tsv_builder: TsvBuilder
    header_builder: TsvHeaderBuilder
