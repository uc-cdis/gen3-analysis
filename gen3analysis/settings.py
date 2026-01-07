from typing import Optional
from cdislogging import get_logger
from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict
import os


def snake_to_pascal(snake_case_string):
    """
    Converts a snake_case string to PascalCase.
    """
    # Replace underscores with spaces, capitalize the first letter of each word,
    # then remove all spaces.
    pascal_case_string = snake_case_string.replace("_", " ").title().replace(" ", "")
    return pascal_case_string


class Settings(BaseSettings):
    GUNICORN_WORKERS: Optional[int] = 1
    HOSTNAME: Optional[str] = ""
    DEBUG: Optional[bool] = False

    # Gen3 services
    ARBORIST_URL: Optional[str] = "http://arborist-service"
    DEPLOYMENT_TYPE: Optional[str] = "prod"

    GUPPY_URL: Optional[str] = "http://guppy-service"

    # Root of the documentation
    URL_PREFIX: Optional[str] = "/analysis/v0"

    # ES connection
    GEN3_ES_ENDPOINT: Optional[str] = "http://gen3-elasticsearch-master:9200"
    ES_USERNAME: Optional[str] = None
    ES_PASSWORD: Optional[str] = None
    ES_API_KEY: Optional[str] = (
        None  # base64, or "id:api_key" (the client supports either)
    )
    ES_VERIFY_SSL: Optional[bool] = False
    ES_CA_CERT: Optional[str] = None
    ES_TIMEOUT: int = 30

    MAX_CASES: Optional[int] = 10000

    # Guppy default indices
    CASE_INDEX: Optional[str] = "case"
    FILE_INDEX: Optional[str] = "file"
    PROJECT_INDEX: Optional[str] = "project"
    GENE_CENTRIC_INDEX: Optional[str] = "gene_centric"
    SSM_CENTRIC_INDEX: Optional[str] = "ssm_centric"
    SSM_OCCURRENCE_CENTRIC_INDEX: Optional[str] = "ssm_occurrence_centric"
    CNV_CENTRIC_INDEX: Optional[str] = "cnv_centric"
    CNV_OCCURRENCE_CENTRIC_INDEX: Optional[str] = "cnv_occurrence_centric"
    CASE_CENTRIC_INDEX: Optional[str] = "case_centric"
    CASE_CENTRIC_AGGREGATION_INDEX: Optional[str] = "case_centric"

    # Elastic search indices for MMRF and GDC
    ES_CASE_CENTRIC_INDEX: Optional[str] = "case_centric"
    ES_CASE_INDEX: Optional[str] = "case"
    ES_FILE_INDEX: Optional[str] = "file"
    ES_PROJECT_INDEX: Optional[str] = "project"
    ES_GENE_CENTRIC_INDEX: Optional[str] = "gene_centric"
    ES_SSM_CENTRIC_INDEX: Optional[str] = "ssm_centric"
    ES_SSM_OCCURRENCE_INDEX: Optional[str] = "ssm_occurrence_centric"
    ES_CNV_CENTRIC_INDEX: Optional[str] = "cnv_centric"
    ES_CNV_OCCURRENCE_INDEX: Optional[str] = "cnv_occurrence_centric"

    # Gene Expression API settings
    # Path to SQLite database with gene/case metadata
    GENE_EXPRESSION_SQLITE_PATH: Optional[str] = (
        "data/mmrf_gene_expression_test_data/schemaless.sqlite3"
    )
    # Path to a directory containing binary expression value files
    GENE_EXPRESSION_DATA_DIR: Optional[str] = (
        "data/mmrf_gene_expression_test_data/mmrf_test_data"
    )
    # Enable/disable gene expression API endpoints
    GENE_EXPRESSION_ENABLED: bool = True

    @classmethod
    def compute_gql_index(cls, index: str) -> str:
        return f"{snake_to_pascal(index)}_{index}"

    @classmethod
    def compute_gql_agg_index(cls, index: str) -> str:
        return f"{snake_to_pascal(index)}__aggregation"

    @computed_field
    @property
    def case_gql(self) -> str:
        return Settings.compute_gql_index(self.CASE_INDEX)

    @computed_field
    @property
    def case_agg_gql(self) -> str:
        return Settings.compute_gql_agg_index(self.CASE_INDEX)

    @computed_field
    @property
    def file_gql(self) -> str:
        return Settings.compute_gql_index(self.FILE_INDEX)

    @computed_field
    @property
    def file_agg_gql(self) -> str:
        return Settings.compute_gql_agg_index(self.FILE_INDEX)

    @computed_field
    @property
    def project_gql(self) -> str:
        return Settings.compute_gql_index(self.PROJECT_INDEX)

    @computed_field
    @property
    def project_agg_gql(self) -> str:
        return Settings.compute_gql_agg_index(self.PROJECT_INDEX)

    @computed_field
    @property
    def gene_centric_gql(self) -> str:
        return Settings.compute_gql_index(self.GENE_CENTRIC_INDEX)

    @computed_field
    @property
    def gene_centric_agg_gql(self) -> str:
        return Settings.compute_gql_agg_index(self.GENE_CENTRIC_INDEX)

    @computed_field
    @property
    def ssm_centric_gql(self) -> str:
        return Settings.compute_gql_index(self.SSM_CENTRIC_INDEX)

    @computed_field
    @property
    def ssm_centric_agg_gql(self) -> str:
        return Settings.compute_gql_agg_index(self.SSM_CENTRIC_INDEX)

    @computed_field
    @property
    def ssm_occurrence_centric_gql(self) -> str:
        return Settings.compute_gql_index(self.SSM_OCCURRENCE_CENTRIC_INDEX)

    @computed_field
    @property
    def ssm_occurrence_centric_agg_gql(self) -> str:
        return Settings.compute_gql_agg_index(self.SSM_OCCURRENCE_CENTRIC_INDEX)

    @computed_field
    @property
    def cnv_centric_gql(self) -> str:
        return Settings.compute_gql_index(self.CNV_CENTRIC_INDEX)

    @computed_field
    @property
    def cnv_centric_agg_gql(self) -> str:
        return Settings.compute_gql_agg_index(self.CNV_CENTRIC_INDEX)

    @computed_field
    @property
    def cnv_occurrence_centric_gql(self) -> str:
        return Settings.compute_gql_index(self.CNV_OCCURRENCE_CENTRIC_INDEX)

    @computed_field
    @property
    def cnv_occurrence_centric_agg_gql(self) -> str:
        return Settings.compute_gql_agg_index(self.CNV_OCCURRENCE_CENTRIC_INDEX)

    @computed_field
    @property
    def case_centric_gql(self) -> str:
        return Settings.compute_gql_index(self.CASE_CENTRIC_INDEX)

    @computed_field
    @property
    def case_centric_agg_gql(self) -> str:
        return Settings.compute_gql_agg_index(self.CASE_CENTRIC_INDEX)

    _env_path = os.path.join(os.path.dirname(__file__), "..", ".env")

    model_config = SettingsConfigDict(
        # If the .env file is missing (common in CI / containers), just skip it.
        env_file=_env_path if os.path.exists(_env_path) else None,
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()

logger = get_logger("gen3-analysis", log_level="debug" if settings.DEBUG else "debug")
