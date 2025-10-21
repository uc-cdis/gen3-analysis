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

    MAX_CASES: Optional[int] = 10000

    GRAPHQL_CASE_INDEX: Optional[str] = "Case_case"
    GRAPHQL_FILE_INDEX: Optional[str] = "File_file"
    GRAPHQL_PROJECT_INDEX: Optional[str] = "Project_project"
    GRAPHQL_GENE_CENTRIC_INDEX: Optional[str] = "GeneCentric_gene_centric"
    GRAPHQL_SSM_CENTRIC_INDEX: Optional[str] = "SsmCentric_ssm_centric"
    GRAPHQL_SSM_OCCURRENCE_CENTRIC_INDEX: Optional[str] = (
        "SsmOccurrenceCentric_ssm_occurrence_centric"
    )
    GRAPHQL_CNV_CENTRIC_INDEX: Optional[str] = "CnvCentric_cnv_centric"
    GRAPHQL_CASE_CENTRIC_INDEX: Optional[str] = "CaseCentric_case_centric"
    GRAPHQL_CASE_CENTRIC_AGGREGATION_INDEX: Optional[str] = "CaseCentric__aggregation"

    ES_CASE_CENTRIC_INDEX: Optional[str] = "mmrf-commpass-ia14_viz_open_1__case_centric"
    ES_CASE_INDEX: Optional[str] = "ia24-20251017_case"
    ES_FILE_INDEX: Optional[str] = "ia24-20251017_file"
    ES_PROJECT_INDEX: Optional[str] = "ia24-20251017_project"
    ES_GENE_CENTRIC_INDEX: Optional[str] = "mmrf-commpass-ia14_viz_open_1__gene_centric"
    ES_SSM_CENTRIC_INDEX: Optional[str] = "mmrf-commpass-ia14_viz_open_1__ssm_centric"
    ES_CNV_CENTRIC_INDEX: Optional[str] = "mmrf-commpass-ia14_viz_open_1__cnv_centric"

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
