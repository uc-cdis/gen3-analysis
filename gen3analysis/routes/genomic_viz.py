from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional, Literal, AsyncIterator
import httpx
import pysam
from pydantic import BaseModel, Field
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.responses import FileResponse
from starlette.responses import StreamingResponse
import asyncio
from gen3analysis.settings import logger, settings
from urllib.parse import urlparse
import re
import subprocess
import tempfile
from fastapi import APIRouter


def extract_s3_key_from_presigned_url(presigned_url: str) -> str:
    """
    Extract the S3 key (UUID-based path) from a presigned URL.
    Validates UUID format in the path.

    Args:
        presigned_url: AWS S3 presigned URL

    Returns:
        S3 key in format: "uuid/filename.ext"

    Raises:
        ValueError: If URL doesn't contain valid UUID path structure
    """
    parsed = urlparse(presigned_url)
    s3_key = parsed.path.lstrip("/")

    # Optional: Validate UUID format
    uuid_pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/"
    if not re.match(uuid_pattern, s3_key):
        raise ValueError(f"URL path doesn't start with UUID format: {s3_key}")

    return s3_key


def get_cache_key(url: str) -> str:
    """Generate cache key from S3 URL"""
    s3_key = extract_s3_key_from_presigned_url(url)
    return s3_key.replace("/", "_")


def get_file_path(cache_key: str, extension: str) -> Path:
    return settings.CACHE_DIR / f"{cache_key}{extension}"


def get_bam_path(cache_key: str) -> Path:
    return settings.CACHE_DIR / cache_key


def get_bai_path(cache_key: str) -> Path:
    return settings.CACHE_DIR / f"{cache_key}.bai"


async def download_file(url: str, output_path: Path) -> None:
    """Download file from presigned URL"""
    async with httpx.AsyncClient(timeout=300.0) as client:
        async with client.stream("GET", url) as response:
            response.raise_for_status()
            with open(output_path, "wb") as f:
                async for chunk in response.aiter_bytes(chunk_size=8192):
                    f.write(chunk)


def create_bam_index(bam_path: Path) -> Path:
    """Create BAM index using pysam"""
    pysam.index(str(bam_path))
    bai_path = Path(str(bam_path) + ".bai")
    return bai_path


def process_idat_to_bed(
    idat_grn_path: Path,
    idat_red_path: Path,
    output_format: Literal["bed", "seg", "vcf"],
    array_type: str = "IlluminaHumanMethylation450k",
) -> Path:
    """
    Process IDAT files using minfi (R/Bioconductor)
    Requires R with minfi installed
    """
    cache_key = idat_grn_path.stem.replace("_Grn", "")
    output_path = get_file_path(cache_key, f".{output_format}")

    if output_path.exists():
        return output_path

    # Create R script for processing
    r_script = f"""
    library(minfi)
    library(rtracklayer)

    # Read IDAT files
    base_dir <- "{idat_grn_path.parent}"
    base_name <- "{cache_key}"

    # Read the RGChannelSet
    rgSet <- read.metharray(base_name, basedir = base_dir, verbose = TRUE)

    # Preprocess
    mSet <- preprocessIllumina(rgSet)

    # Get beta values
    beta <- getBeta(mSet)

    # Get genomic positions
    annotation <- getAnnotation(mSet)

    if ("{output_format}" == "bed") {{
        # Create BED file with methylation beta values
        bed_df <- data.frame(
            chrom = annotation$chr,
            start = annotation$pos - 1,  # BED is 0-based
            end = annotation$pos,
            name = rownames(beta),
            score = round(beta[,1] * 1000)  # Scale to 0-1000
        )

        # Filter and sort
        bed_df <- bed_df[!is.na(bed_df$chrom) & !is.na(bed_df$start),]
        bed_df <- bed_df[order(bed_df$chrom, bed_df$start),]

        write.table(bed_df, "{output_path}",
                    sep = "\\t", quote = FALSE,
                    row.names = FALSE, col.names = FALSE)
    }} else if ("{output_format}" == "seg") {{
        # Create SEG file (segmented copy number-like format)
        # Using beta values as proxy for methylation segments
        library(DNAcopy)

        CNA_obj <- CNA(genomdat = beta[,1],
                       chrom = annotation$chr,
                       maploc = annotation$pos,
                       data.type = "logratio")

        smoothed <- smooth.CNA(CNA_obj)
        segments <- segment(smoothed, verbose = 1)

        seg_df <- segments$output
        write.table(seg_df, "{output_path}",
                    sep = "\\t", quote = FALSE,
                    row.names = FALSE)
    }} else if ("{output_format}" == "vcf") {{
        # Extract SNP genotypes if available
        # Note: This requires SNP probes on the array
        snps <- getSnpBeta(rgSet)

        # Simple VCF generation (simplified)
        # Real implementation would need proper VCF formatting
        stop("VCF generation from IDAT requires additional SNP annotation")
    }}

    quit(save = "no", status = 0)
    """

    # Write R script to temp file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".R", delete=False) as f:
        f.write(r_script)
        r_script_path = f.name

    try:
        # Run R script
        result = subprocess.run(
            ["Rscript", r_script_path], capture_output=True, text=True, timeout=600
        )

        if result.returncode != 0:
            raise Exception(f"R processing failed: {result.stderr}")

        return output_path
    finally:
        os.unlink(r_script_path)


def process_idat_python(
    idat_grn_path: Path, idat_red_path: Path, output_format: Literal["bed", "bedgraph"]
) -> Path:
    """
    Alternative: Process IDAT using methylprep (Python package)
    This is simpler but has fewer features than minfi
    """
    try:
        from methylprep import run_pipeline
        import pandas as pd
    except ImportError:
        raise HTTPException(
            status_code=500,
            detail="methylprep not installed. Install with: pip install methylprep",
        )

    cache_key = idat_grn_path.stem.replace("_Grn", "")
    output_path = get_file_path(cache_key, f".{output_format}")

    if output_path.exists():
        return output_path

    # Process IDAT files
    data_dir = idat_grn_path.parent

    # Run preprocessing pipeline
    df = run_pipeline(data_dir, export=True, betas=True)

    # Load manifest for genomic positions
    # This requires downloading the appropriate manifest
    # For now, using a simplified approach

    # Create BED format output
    if output_format == "bed":
        # df should have probe IDs as index
        # You'd need to join with manifest to get genomic positions
        # Simplified example:
        bed_data = []
        for probe_id in df.index:
            # In reality, look up probe genomic position from manifest
            # This is a placeholder
            bed_data.append(
                {
                    "chrom": "chr1",  # Placeholder
                    "start": 0,  # Placeholder
                    "end": 1,  # Placeholder
                    "name": probe_id,
                    "score": int(df.loc[probe_id].values[0] * 1000),
                }
            )

        bed_df = pd.DataFrame(bed_data)
        bed_df.to_csv(output_path, sep="\t", header=False, index=False)

    return output_path


genomic_viz = APIRouter()


class BAMIndexRequest(BaseModel):
    url: str = Query(..., description="Presigned URL for BAM file")


@genomic_viz.post("/bam/index")
async def get_bam_index(
    body: BAMIndexRequest,
):
    """Generate or retrieve a cached BAM index file"""

    bam_url = body.url

    try:
        cache_key = get_cache_key(bam_url)
        bam_path = get_file_path(cache_key, ".bam")
        bai_path = get_file_path(cache_key, ".bam.bai")

        if bai_path.exists():
            bai_path.touch()
            return FileResponse(
                bai_path,
                media_type="application/octet-stream",
                filename=f"{cache_key}.bam.bai",
            )

        if not bam_path.exists():
            print(f"Downloading BAM from {bam_url[:50]}...")
            await download_file(bam_url, bam_path)

        logger.info(f"Creating index for {cache_key}...")
        create_bam_index(bam_path)

        return FileResponse(
            bai_path,
            media_type="application/octet-stream",
            filename=f"{cache_key}.bam.bai",
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to process BAM: {str(e)}")


async def download_file_with_progress(
    url: str, output_path: Path, progress_callback=None
) -> None:
    """Download file with progress reporting"""
    async with httpx.AsyncClient(timeout=300.0) as client:
        async with client.stream("GET", url) as response:
            response.raise_for_status()

            total_size = int(response.headers.get("content-length", 0))
            downloaded = 0

            with open(output_path, "wb") as f:
                async for chunk in response.aiter_bytes(chunk_size=8192):
                    f.write(chunk)
                    downloaded += len(chunk)

                    if progress_callback and total_size > 0:
                        progress = (downloaded / total_size) * 100
                        await progress_callback(
                            "download", progress, downloaded, total_size
                        )


def create_bam_index_with_progress(bam_path: Path, progress_callback=None) -> Path:
    """
    Create BAM index with progress reporting
    Note: pysam.index doesn't provide progress, so we'll estimate based on file size
    """
    import threading
    import time

    bai_path = Path(str(bam_path) + ".bai")

    if progress_callback:
        # Get file size for estimation
        file_size = bam_path.stat().st_size

        # Start indexing in a thread
        index_complete = threading.Event()

        def index_worker():
            pysam.index(str(bam_path))
            index_complete.set()

        thread = threading.Thread(target=index_worker)
        thread.start()

        # Simulate progress (since pysam doesn't provide real progress)
        start_time = time.time()
        # Estimate: ~1-2 minutes per GB
        estimated_duration = (file_size / 1e9) * 90  # 90 seconds per GB

        while not index_complete.is_set():
            elapsed = time.time() - start_time
            progress = min((elapsed / estimated_duration) * 100, 95)  # Cap at 95%
            progress_callback("indexing", progress, elapsed, estimated_duration)
            time.sleep(0.5)

        thread.join()
        progress_callback("indexing", 100, estimated_duration, estimated_duration)
    else:
        pysam.index(str(bam_path))

    return bai_path


async def process_bam_with_progress(bam_url: str) -> AsyncIterator[str]:
    """
    Process BAM file and yield SSE progress events
    """
    try:
        cache_key = get_cache_key(bam_url)
        bam_path = get_bam_path(cache_key)
        bai_path = get_bai_path(cache_key)

        # Check if already cached
        if bai_path.exists():
            yield f"data: {json.dumps({'status': 'complete', 'progress': 100, 'message': 'Index already cached'})}\n\n"
            return

        # Progress callback for async operations
        async def send_progress(
            stage: str, progress: float, current: float, total: float
        ):
            event_data = {
                "status": "processing",
                "stage": stage,
                "progress": round(progress, 2),
                "current": round(current, 2),
                "total": round(total, 2),
            }
            yield f"data: {json.dumps(event_data)}\n\n"

        # Download if needed
        if not bam_path.exists():
            yield f"data: {json.dumps({'status': 'processing', 'stage': 'download', 'progress': 0, 'message': 'Starting download'})}\n\n"

            # Download with progress
            async with httpx.AsyncClient(timeout=300.0) as client:
                async with client.stream("GET", bam_url) as response:
                    response.raise_for_status()

                    total_size = int(response.headers.get("content-length", 0))
                    downloaded = 0

                    with open(bam_path, "wb") as f:
                        async for chunk in response.aiter_bytes(chunk_size=65536):
                            f.write(chunk)
                            downloaded += len(chunk)

                            if total_size > 0:
                                progress = (downloaded / total_size) * 100
                                event_data = {
                                    "status": "processing",
                                    "stage": "download",
                                    "progress": round(progress, 2),
                                    "downloaded_mb": round(downloaded / 1024 / 1024, 2),
                                    "total_mb": round(total_size / 1024 / 1024, 2),
                                }
                                yield f"data: {json.dumps(event_data)}\n\n"
                                await asyncio.sleep(0)  # Yield control

        # Index the file
        yield f"data: {json.dumps({'status': 'processing', 'stage': 'indexing', 'progress': 0, 'message': 'Creating index'})}\n\n"

        # Run indexing in thread pool to not block
        import concurrent.futures
        import time

        file_size = bam_path.stat().st_size
        estimated_duration = (file_size / 1e9) * 60  # 60 seconds per GB estimate

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(pysam.index, str(bam_path))

            start_time = time.time()
            while not future.done():
                elapsed = time.time() - start_time
                progress = min((elapsed / estimated_duration) * 100, 95)

                event_data = {
                    "status": "processing",
                    "stage": "indexing",
                    "progress": round(progress, 2),
                    "elapsed_seconds": round(elapsed, 1),
                    "estimated_seconds": round(estimated_duration, 1),
                }
                yield f"data: {json.dumps(event_data)}\n\n"
                await asyncio.sleep(0.5)

            # Get result (will raise if there was an error)
            future.result()

        # Complete
        yield f"data: {json.dumps({'status': 'complete', 'progress': 100, 'message': 'Index created successfully', 'bai_url': f'/bam/index/file?url={bam_url}'})}\n\n"

    except Exception as e:
        yield f"data: {json.dumps({'status': 'error', 'message': str(e)})}\n\n"


@genomic_viz.get("/bam/index/progress")
async def get_bam_index_progress(
    url: str = Query(..., description="Presigned URL for BAM file")
):
    """
    Stream progress updates for BAM index creation using Server-Sent Events
    """
    bam_url = url

    async def event_generator():
        async for event in process_bam_with_progress(bam_url):
            yield event

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Disable nginx buffering
        },
    )


@genomic_viz.get("/bam/index/file")
async def get_bam_index_file(
    url: str = Query(..., description="Presigned URL for BAM file")
):
    """
    Get the BAM index file (after it's been created)
    """
    bam_url = url
    try:
        cache_key = get_cache_key(bam_url)
        bai_path = get_bai_path(cache_key)

        if not bai_path.exists():
            raise HTTPException(
                status_code=404,
                detail="Index not found. Call /bam/index/progress first",
            )

        return FileResponse(
            bai_path, media_type="application/octet-stream", filename=f"{cache_key}.bai"
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@genomic_viz.get("/bam/index/status")
async def get_bam_index_status(
    url: str = Query(..., description="Presigned URL for BAM file")
):
    """
    Check if BAM index already exists in cache
    """
    bam_url = url
    cache_key = get_cache_key(bam_url)
    bai_path = get_bai_path(cache_key)

    return {
        "cache_key": cache_key,
        "exists": bai_path.exists(),
        "bai_url": f"/bam/index/file?url={bam_url}" if bai_path.exists() else None,
    }
