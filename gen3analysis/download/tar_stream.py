"""
FastAPI endpoint for streaming tar files containing queried data objects.
"""

import asyncio
import io
import json
import tarfile
from typing import List, Optional, AsyncGenerator
from datetime import datetime


# Simulated data source - replace with your actual data source
# (database, Elasticsearch, file system, etc.)
MOCK_DATA_OBJECTS = [
    {"id": "obj_001", "name": "Sample Object 1", "data": {"value": 100, "type": "A"}},
    {"id": "obj_002", "name": "Sample Object 2", "data": {"value": 200, "type": "B"}},
    {"id": "obj_003", "name": "Sample Object 3", "data": {"value": 300, "type": "A"}},
    {"id": "obj_004", "name": "Sample Object 4", "data": {"value": 400, "type": "C"}},
    {"id": "obj_005", "name": "Sample Object 5", "data": {"value": 500, "type": "B"}},
]


async def query_data_objects(
    object_ids: Optional[List[str]] = None,
    object_type: Optional[str] = None,
    limit: int = 100,
) -> List[dict]:
    """
    Query data objects based on parameters.
    Replace this with your actual data retrieval logic.
    """

    results = MOCK_DATA_OBJECTS.copy()

    # Filter by IDs if provided
    if object_ids:
        results = [obj for obj in results if obj["id"] in object_ids]

    # Filter by type if provided
    if object_type:
        results = [obj for obj in results if obj["data"].get("type") == object_type]

    # Apply limit
    results = results[:limit]

    return results


def create_tar_info(name: str, content: bytes) -> tarfile.TarInfo:
    """
    Create a TarInfo object for a file entry.
    """
    tarinfo = tarfile.TarInfo(name=name)
    tarinfo.size = len(content)
    tarinfo.mtime = datetime.now().timestamp()
    tarinfo.mode = 0o644
    return tarinfo


async def generate_tar_stream(
    data_objects: List[dict], compress: bool = False
) -> AsyncGenerator[bytes, None]:
    """
    Generator function that yields tar file chunks.
    This streams the tar file creation without loading everything into memory.
    """
    # Create an in-memory buffer
    buffer = io.BytesIO()

    # Determine tar mode (with or without gzip compression)
    mode = "w:gz" if compress else "w"

    # Create tar file
    with tarfile.open(fileobj=buffer, mode=mode) as tar:
        for obj in data_objects:
            # Convert data object to JSON or TSV
            content = json.dumps(obj, indent=2).encode("utf-8")

            # Create filename from object ID
            filename = f"{obj['id']}.json"

            # Create tar info
            tarinfo = create_tar_info(filename, content)

            # Add file to tar
            tar.addfile(tarinfo, io.BytesIO(content))

            # Yield chunks periodically to avoid blocking
            if buffer.tell() > 1024 * 1024:  # 1MB chunks
                buffer.seek(0)
                chunk = buffer.read()
                if chunk:
                    yield chunk
                buffer = io.BytesIO()
                tar.fileobj = buffer

    # Yield any remaining data
    buffer.seek(0)
    remaining = buffer.read()
    if remaining:
        yield remaining
