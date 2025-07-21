import traceback

from gen3.auth import Gen3Auth
from gen3analysis.config import logger


class Gen3AuthToken:
    def __init__(self, endpoint: str):
        self.endpoint = endpoint
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
