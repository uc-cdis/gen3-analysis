from typing import Union

from authutils.token.fastapi import access_token
from fastapi import HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from gen3.auth import Gen3Auth
from gen3authz.client.arborist.errors import ArboristError
from starlette.requests import Request
from starlette.status import (
    HTTP_401_UNAUTHORIZED,
    HTTP_403_FORBIDDEN,
)
import traceback

from gen3analysis import config
from gen3analysis.config import logger

# auto_error=False prevents FastAPI from raising a 403 when the request
# is missing an Authorization header. Instead, we want to return a 401
# to signify that we did not receive valid credentials
bearer = HTTPBearer(auto_error=False)


class Gen3SdkAuth:
    def __init__(self, endpoint: str):
        try:
            self.auth = Gen3Auth(endpoint=endpoint)
        except:
            traceback.print_exc()
            logger.warning(
                "Unable to initialize Gen3Auth instance. Authorization checks will not work. Proceeding anyway..."
            )
            self.auth = None

    async def get_access_token(self) -> str:
        return self.auth.get_access_token()


class Auth:
    def __init__(
        self,
        api_request: Request,
        bearer_token: HTTPAuthorizationCredentials = Security(bearer),
    ) -> None:
        self.app = api_request.app
        self.arborist_client = self.app.state.arborist_client
        self.bearer_token = bearer_token

        # if config.MOCK_AUTH:
        #     logger.warning(
        #         "Mock authentication and authorization are enabled! 'MOCK_AUTH' should NOT be enabled in production!"
        #     )

    async def get_access_token(self) -> str:
        # if config.MOCK_AUTH:
        #     return "123"

        token = (
            self.bearer_token.credentials
            if self.bearer_token and hasattr(self.bearer_token, "credentials")
            else None
        )
        if not token and config.DEPLOYMENT_TYPE == "dev":
            token = await self.auth.get_access_token()

        return token

    async def get_token_claims(self) -> dict:
        # if config.MOCK_AUTH:
        #     return {"sub": 64, "context": {"user": {"name": "mocked-user"}}}

        if not self.bearer_token:
            err_msg = "Must provide an access token"
            logger.error(err_msg)
            raise HTTPException(
                HTTP_401_UNAUTHORIZED,
                err_msg,
            )

        try:
            token_claims = await access_token(
                "user", "openid", audience="openid", purpose="access"
            )(self.bearer_token)
        except Exception as e:
            err_msg = "Could not verify, parse, and/or validate provided access token"
            logger.error(
                f"{err_msg}:\n{e.detail if hasattr(e, 'detail') else e}",
                exc_info=True,
            )
            raise HTTPException(HTTP_401_UNAUTHORIZED, err_msg)

        return token_claims

    async def authorize(
        self,
        method: str,
        resources: list,
        throw: bool = True,
    ) -> bool:
        # if config.MOCK_AUTH:
        #     return True

        token = self.get_access_token()
        try:
            authorized = await self.arborist_client.auth_request(
                token, "gen3-analysis", method, resources
            )
        except ArboristError as e:
            logger.error(f"Error while talking to arborist: {e}")
            authorized = False

        if not authorized:
            token_claims = await self.get_token_claims() if token else {}
            user_id = token_claims.get("sub")
            client_id = token_claims.get("azp")
            logger.error(
                f"Authorization error for user '{user_id}' / client '{client_id}': token must have '{method}' access on {resources} for service 'gen3-analysis'."
            )
            if throw:
                raise HTTPException(
                    HTTP_403_FORBIDDEN,
                    "Permission denied",
                )

        return authorized
