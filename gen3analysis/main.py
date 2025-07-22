import asyncio
from typing import Optional
from contextlib import asynccontextmanager
from importlib.metadata import version
from gen3analysis.clients import CSRFTokenCache, GuppyGQLClient, GDCGQLClient
import fastapi
from fastapi import FastAPI, APIRouter

from gen3analysis.gen3.auth import Gen3AuthToken
from gen3analysis.routes.compare import compare
from gen3analysis.routes.survival import survival
from gen3analysis.routes.survivalGen3 import survivalGen3
from gen3analysis import config
from gen3analysis.config import logger
from gen3analysis.routes.basic import basic_router

route_aggregator = APIRouter()


route_definitions = [
    (basic_router, "", ["Basic"]),
    (compare, "/compare", ["Compare"]),
    (survival, "/survival", ["Survival"]),
    (survivalGen3, "/survivalGen3", ["survivalGen3"]),
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
    global csrf_cache, guppy_client, gdc_graphql_client, gen_auth_token
    csrf_cache = CSRFTokenCache(
        rest_api_url="https://dev-virtuallab.themmrf.org/_status",
        token_ttl_seconds=3600,  # 1 hour
    )

    gen_auth_token = Gen3AuthToken(
        endpoint="https://dev-virtuallab.themmrf.org",
    )

    guppy_client = GuppyGQLClient(
        graphql_url="https://dev-virtuallab.themmrf.org/guppy/graphql",
        csrf_cache=csrf_cache,
        gen3_auth_token=gen_auth_token,
    )

    gdc_graphql_client = GDCGQLClient(
        graphql_url="https://portal.gdc.cancer.gov/auth/api/v0/graphql",
    )

    app.state.csrf_cache = csrf_cache
    app.state.guppy_client = guppy_client
    app.state.gdc_graphql_client = gdc_graphql_client
    app.state.gen_auth_token = gen_auth_token

    yield

    # teardown

    # teardown
    app.state.csrf_cache = None
    app.state.guppy_client = None
    app.state.gdc_graphql_client = None
    app.state.gen_auth_token = None

    # NOTE: multiprocess.mark_process_dead is called by the gunicorn "child_exit" function for each worker  #
    # "child_exit" is defined in the gunicorn.conf.py


# async def check_arborist_is_healthy(app_with_setup):
#     """
#     Checks that we can talk to arborist
#     Args:
#         app_with_setup (FastAPI): the fastapi app with arborist client
#
#     """
#     logger.debug("Startup policy engine (Arborist) connection test initiating...")
#     arborist_client = app_with_setup.state.arborist_client
#     if not arborist_client.healthy():
#         logger.exception(
#             "Startup policy engine (Arborist) connection test FAILED. Unable to connect to the policy engine."
#         )
#         logger.debug("Arborist is unhealthy")
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
    fastapi_app.add_middleware(ClientDisconnectMiddleware)

    return fastapi_app


class ClientDisconnectMiddleware:
    def __init__(self, app):
        self._app = app

    async def __call__(self, scope, receive, send):
        loop = asyncio.get_running_loop()
        rv = loop.create_task(self._app(scope, receive, send))
        waiter = None
        cancelled = False
        if scope["type"] == "http":

            def add_close_watcher():
                nonlocal waiter

                async def wait_closed():
                    nonlocal cancelled
                    while True:
                        message = await receive()
                        if message["type"] == "http.disconnect":
                            if not rv.done():
                                cancelled = True
                                rv.cancel()
                            break

                waiter = loop.create_task(wait_closed())

            scope["add_close_watcher"] = add_close_watcher
        try:
            await rv
        except asyncio.CancelledError:
            if not cancelled:
                raise
        if waiter and not waiter.done():
            waiter.cancel()


app_instance = get_app()
