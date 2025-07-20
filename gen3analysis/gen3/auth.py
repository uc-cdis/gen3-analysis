from gen3.auth import Gen3Auth


class Gen3AuthToken:
    def __init__(self, endpoint: str):
        self.endpoint = endpoint
        self.auth = Gen3Auth(endpoint=endpoint)

    async def get_access_token(self) -> str:
        return self.auth.get_access_token()
