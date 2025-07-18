from fastapi import Request, HTTPException
from gen3analysis.gen3.guppyQuery import GuppyGQLClient


def get_guppy_client(request: Request) -> GuppyGQLClient:
    """
    Dependency function to get the global GuppyGQLClient instance.

    Returns:
        GuppyGQLClient: The global guppy client instance

    Raises:
        HTTPException: If the client is not initialized
    """
    guppy_client = getattr(request.app.state, "guppy_client", None)

    if guppy_client is None:
        raise HTTPException(status_code=500, detail="GuppyGQLClient not initialized")
    return guppy_client
