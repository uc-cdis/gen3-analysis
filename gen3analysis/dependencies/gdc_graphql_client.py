from fastapi import Request, HTTPException
from gen3analysis.gdc.graphqlQuery import GDCGQLClient


def get_gdc_graphql_client(request: Request) -> GDCGQLClient:
    """
    Dependency function to get the global GuppyGQLClient instance.

    Returns:
        GDCGQLClient: The global gdc graphql client instance

    Raises:
        HTTPException: If the client is not initialized
    """
    gdc_graphql_client = getattr(request.app.state, "gdc_graphql_client", None)
    if gdc_graphql_client is None:
        raise HTTPException(status_code=500, detail="GDCGQLClient not initialized")
    return gdc_graphql_client
