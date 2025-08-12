import asyncio
from contextlib import asynccontextmanager
from importlib.metadata import version
import os

from cdislogging import get_logger
from gen3authz.client.arborist.async_client import ArboristClient
import fastapi
from fastapi import FastAPI, APIRouter

from gen3analysis.auth import Gen3SdkAuth
from gen3analysis.gen3.guppyQuery import GuppyGQLClient
from gen3analysis.gdc.graphqlQuery import GDCGQLClient
from gen3analysis.routes.compare import compare

from gen3analysis.routes.survival import survival
from gen3analysis import config

from gen3analysis.routes.basic import basic_router

route_aggregator = APIRouter()


route_definitions = [
    (basic_router, "", ["Basic"]),
    (compare, "/compare", ["Compare"]),
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
    if config.DEPLOYMENT_TYPE == "prod":
        guppy_url = "http://guppy-service"
        revproxy_url = "http://revproxy-service"
    else:
        guppy_url = f"{config.HOSTNAME}/guppy"
        revproxy_url = f"{config.HOSTNAME}"

    guppy_client = GuppyGQLClient(
        graphql_url=f"{guppy_url}/graphql", csrf_token_url=revproxy_url
    )

    gdc_graphql_client = GDCGQLClient(
        graphql_url="https://portal.gdc.cancer.gov/auth/api/v0/graphql",
    )

    app.state.guppy_client = guppy_client
    app.state.gdc_graphql_client = gdc_graphql_client
    app.state.gen3_sdk_auth = None
    if config.DEPLOYMENT_TYPE == "dev":
        app.state.gen3_sdk_auth = Gen3SdkAuth(endpoint=config.HOSTNAME)

    app.state.arborist_client = ArboristClient(
        arborist_base_url=config.ARBORIST_URL,
        logger=get_logger(
            "gen3analysis.gen3authz", log_level="debug" if config.DEBUG else "info"
        ),
    )

    yield

    # teardown
    app.state.guppy_client = None
    app.state.gdc_graphql_client = None
    app.state.gen3_sdk_auth = None
    app.state.arborist_client = None

    # NOTE: multiprocess.mark_process_dead is called by the gunicorn "child_exit" function for each
    # worker. "child_exit" is defined in the gunicorn.conf.py


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
