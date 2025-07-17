from typing import Optional
from contextlib import asynccontextmanager
from importlib.metadata import version
from gen3analysis.gen3.csrfTokenCache import CSRFTokenCache

import fastapi
from cdislogging import get_logger
from fastapi import FastAPI, APIRouter

from gen3analysis.routes.survival import survival


from gen3analysis import config

from gen3analysis.config import logging
from gen3analysis.routes.basic import basic_router

route_aggregator = APIRouter()

csrf_cache: Optional[CSRFTokenCache] = None

route_definitions = [
    (basic_router, "", ["Basic"]),
    (survival, "/survival", ["Survival"]),
]

for router, prefix, tags in route_definitions:
    route_aggregator.include_router(router, prefix=prefix, tags=tags)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Parse the configuration, setup and instantiate necessary classes.

    This is FastAPI's way of dealing with startup logic before the app
    starts receiving requests.

    https://fastapi.tiangolo.com/advanced/events/#lifespan

    Args:
        app (fastapi.FastAPI): The FastAPI app object
    """
    # startup
    app_with_setup = app
    global csrf_cache
    csrf_cache = CSRFTokenCache(
        rest_api_url="https://revproxy-service/_status",
        token_ttl_seconds=3600,  # 1 hour
    )
    yield

    # teardown

    # NOTE: multiprocess.mark_process_dead is called by the gunicorn "child_exit" function for each worker  #
    # "child_exit" is defined in the gunicorn.conf.py


# async def check_arborist_is_healthy(app_with_setup):
#     """
#     Checks that we can talk to arborist
#     Args:
#         app_with_setup (FastAPI): the fastapi app with arborist client
#
#     """
#     logging.debug("Startup policy engine (Arborist) connection test initiating...")
#     arborist_client = app_with_setup.state.arborist_client
#     if not arborist_client.healthy():
#         logging.exception(
#             "Startup policy engine (Arborist) connection test FAILED. Unable to connect to the policy engine."
#         )
#         logging.debug("Arborist is unhealthy")
#         raise Exception("Arborist unhealthy, aborting...")


# async def add_arborist_client(app):
#     """
#     Helper function to add arborist client
#     Args:
#         app (FastAPI): the initial instance of the fast api app
#     """
#     app.state.arborist_client = ArboristClient(
#         arborist_base_url=config.ARBORIST_URL,
#         logger=get_logger("user_syncer.arborist_client"),
#         authz_provider="user-sync",
#     )
#     return app


def get_app() -> fastapi.FastAPI:
    """
    Return the web framework app object after adding routes

    Returns:
        fastapi.FastAPI: The FastAPI app object
    """

    fastapi_app = FastAPI(
        title="Gen3 Analysis Service",
        version=version("gen3analysis"),
        debug=config.DEBUG,
        root_path=config.URL_PREFIX,
        lifespan=lifespan,
    )
    fastapi_app.include_router(route_aggregator)

    return fastapi_app


app_instance = get_app()
