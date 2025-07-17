from rich import print_json
from cdiserrors import InternalError, UserError
import asyncio
from gen3analysis.gdc.graphqlQuery import GDCGQLClient
from gen3analysis.samples.cohortComparision.cohortFilters_GDC import (
    Cohort1_GDC,
    Cohort2_GDC,
)

gdc_client = GDCGQLClient("https://portal.gdc.cancer.gov/auth/api/v0/graphql")

intersection_filters = {
    "op": "and",
    "content": [Cohort1_GDC, Cohort2_GDC],
}

VennDiagramQuery_GDC = """
  query VennDiagram(
    $cohort1: FiltersArgument
    $cohort2: FiltersArgument
    $intersectionFilters: FiltersArgument
  ) {
    viewer {
      explore {
        set1: cases {
          hits(filters: $cohort1, first: 0) {
            total
          }
        }
        set2: cases {
          hits(filters: $cohort2,  first: 0) {
            total
          }
        }
        intersection: cases {
          hits(filters: $intersectionFilters,  first: 0) {
            total
          }
        }
      }
    }
  }"""

GDC_MMRFComparison_GQL_Query_Variables = {
    "cohort1": Cohort1_GDC,
    "cohort2": Cohort2_GDC,
    "intersectionFilters": intersection_filters,
}


async def get_venn_data():
    data = await gdc_client.execute(
        query=VennDiagramQuery_GDC,
        variables=GDC_MMRFComparison_GQL_Query_Variables,
    )
    if data.get("error"):
        raise InternalError(data.get("error"))
    dataRoot = data.get("data", {}).get("viewer", {}).get("explore", {})
    return dataRoot


if __name__ == "__main__":
    print_json(data=GDC_MMRFComparison_GQL_Query_Variables)
    data = asyncio.run(get_venn_data())
    print_json(data=data)
