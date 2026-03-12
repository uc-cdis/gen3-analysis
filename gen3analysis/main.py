import asyncio
from contextlib import asynccontextmanager
from importlib.metadata import version

import fastapi
from cdislogging import get_logger
from fastapi import FastAPI, APIRouter
from fastapi.exceptions import RequestValidationError
from gen3authz.client.arborist.async_client import ArboristClient
from starlette.requests import Request
from starlette.responses import JSONResponse

from gen3analysis.auth import Gen3SdkAuth
from gen3analysis.gen3.es_client import get_nested_registry
from gen3analysis.gen3.guppyQuery import GuppyGQLClient
from gen3analysis.routes.basic import basic_router
from gen3analysis.routes.cases import cases
from gen3analysis.routes.cohorts import cohorts
from gen3analysis.routes.compare import compare
from gen3analysis.routes.files import files
from gen3analysis.routes.gene_expression import gene_expression
from gen3analysis.routes.genomic import genomic
from gen3analysis.routes.ssm_occurrence import ssms_occurrence
from gen3analysis.routes.ssms import ssms
from gen3analysis.routes.cnv import cnv
from gen3analysis.routes.cnv_occurrence import cnv_occurrence
from gen3analysis.routes.survival import survival
from gen3analysis.settings import settings, logger

route_aggregator = APIRouter()


route_definitions = [
    (basic_router, "", ["Basic"]),
    (compare, "/compare", ["Compare"]),
    (survival, "/survival", ["Survival"]),
    (cohorts, "/cohorts", ["Cohorts"]),
    (genomic, "/genomic", ["Genomic"]),
    (cases, "/cases", ["Cases"]),
    (files, "/files", ["Files"]),
    (ssms, "/ssms", ["SSMS"]),
    (ssms_occurrence, "/ssms_occurrence", ["SSMS Occurrence"]),
    (gene_expression, "/gene_expression", ["Gene Expression"]),
    (cnv, "/cnv", ["CNV"]),
    (cnv_occurrence, "/cnv_occurrence", ["CNV Occurrence"]),
]

for router, prefix, tags in route_definitions:
    route_aggregator.include_router(router, prefix=prefix, tags=tags)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Parse the configuration, setup, and instantiate necessary classes.

    This is FastAPI's way of dealing with startup logic before the app
    starts receiving requests.

    https://fastapi.tiangolo.com/advanced/events/#lifespan

    Args:
        app (fastapi.FastAPI): The FastAPI app object
    """
    # startup
    if settings.DEPLOYMENT_TYPE == "prod":
        guppy_url = "http://guppy-service"
        revproxy_url = "http://revproxy-service"
    else:
        guppy_url = f"{settings.GUPPY_URL}"
        revproxy_url = f"{settings.HOSTNAME}"

    guppy_client = GuppyGQLClient(
        graphql_url=f"{guppy_url}/graphql", csrf_token_url=revproxy_url
    )

    get_nested_registry()

    app.state.guppy_client = guppy_client
    app.state.gen3_sdk_auth = None
    if settings.DEPLOYMENT_TYPE == "dev":
        app.state.gen3_sdk_auth = Gen3SdkAuth(endpoint=settings.HOSTNAME)

    app.state.arborist_client = ArboristClient(
        arborist_base_url=settings.ARBORIST_URL,
        logger=get_logger(
            "gen3analysis.gen3authz", log_level="debug" if settings.DEBUG else "info"
        ),
    )

    # Initialize gene expression data store
    if settings.GENE_EXPRESSION_ENABLED:
        from gen3analysis.gene_expression.data_store import GeneExpressionDataStore

        try:
            if (
                settings.GENE_EXPRESSION_SQLITE_PATH
                and settings.GENE_EXPRESSION_DATA_DIR
            ):
                logger.info(
                    "Initializing gene expression data store from %s",
                    settings.GENE_EXPRESSION_DATA_DIR,
                )
                GeneExpressionDataStore.get_instance(
                    sqlite_path=settings.GENE_EXPRESSION_SQLITE_PATH,
                    data_dir=settings.GENE_EXPRESSION_DATA_DIR,
                )
                logger.info("Gene expression data store initialized successfully")
            else:
                logger.warning(
                    "Gene expression data paths not configured and fallback disabled"
                )
        except Exception as e:
            logger.error("Failed to initialize gene expression data store: %s", e)
            raise

    yield

    # teardown
    if app.state.guppy_client:
        await app.state.guppy_client.close()
    app.state.guppy_client = None
    app.state.gdc_graphql_client = None
    app.state.gen3_sdk_auth = None
    app.state.arborist_client = None

    # Reset gene expression data store
    if settings.GENE_EXPRESSION_ENABLED:
        from gen3analysis.gene_expression.data_store import GeneExpressionDataStore

        GeneExpressionDataStore.reset_instance()

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
        debug=settings.DEBUG,
        root_path=settings.URL_PREFIX,
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


@app_instance.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    errors = []
    for err in exc.errors():
        # err["loc"] like ("body", "filter", 0, "name")
        path = ".".join(str(p) for p in err["loc"])
        errors.append({"path": path, "msg": err["msg"], "type": err["type"]})
    return JSONResponse(status_code=422, content={"detail": errors})
