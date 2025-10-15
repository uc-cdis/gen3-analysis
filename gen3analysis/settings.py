from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # ES connection
    ES_HOSTS: str = Field(..., description="Comma-separated list of hosts")
    ES_USERNAME: Optional[str] = None
    ES_PASSWORD: Optional[str] = None
    ES_API_KEY: Optional[str] = (
        None  # base64, or "id:api_key" (the client supports either)
    )
    ES_VERIFY_SSL: bool = False
    ES_CA_CERT: Optional[str] = None

    CASE_CENTRIC_INDEX: Optional[str] = "case_centric"
    CASE_INDEX: Optional[str] = "case"
    FILE_INDEX: Optional[str] = "file"
    PROJECT_INDEX: Optional[str] = "project"
    GENE_INDEX: Optional[str] = "gene_centric"
    SSM_CENTRIC_INDEX: Optional[str] = "SsmCentric_ssm_centric"

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
