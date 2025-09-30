from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # ES connection
    ES_HOSTS: str = Field(..., description="Comma-separated list of hosts")
    ES_USERNAME: Optional[str] = None
    ES_PASSWORD: Optional[str] = None
    ES_API_KEY: Optional[str] = None  # base64, or "id:api_key" (client supports either)
    ES_VERIFY_SSL: bool = False
    ES_CA_CERT: Optional[str] = None
    ES_PIT_KEEP_ALIVE: str = "1m"

    # Top genes config
    TOP_GENES_INDEX: str
    TOP_GENES_GENE_ID_FIELD: str
    TOP_GENES_CASE_NESTED_PATH: str
    TOP_GENES_CASE_ID_FIELD: str
    TOP_GENES_PROJECT_FIELD: str

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
