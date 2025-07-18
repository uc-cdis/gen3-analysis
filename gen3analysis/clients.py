from typing import Optional
from gen3analysis.gen3.csrfTokenCache import CSRFTokenCache
from gen3analysis.gen3.guppyQuery import GuppyGQLClient  # Add this import
from gen3analysis.gdc.graphqlQuery import GDCGQLClient  # Add this import

csrf_cache: Optional[CSRFTokenCache] = None
guppy_client: Optional[GuppyGQLClient] = None
gdc_graphql_client: Optional[GDCGQLClient] = None
