from rich import print_json
from cdiserrors import InternalError, UserError

import json
import asyncio
from gen3analysis.gdc.graphqlQuery import GDCGQLClient

gdc_client = GDCGQLClient("https://portal.gdc.cancer.gov/auth/api/v0/graphql")

GDC_MMRFComparison_GQL_Query = """query CohortComparison(
    $cohort1: FiltersArgument
    $cohort2: FiltersArgument
    $facets: [String]!
    $interval: Float
  ) {
    viewer {
      explore {
        cohort1: cases {
          hits(filters: $cohort1) {
            total
          }
          facets(filters: $cohort1, facets: $facets)
          aggregations(filters: $cohort1) {
            diagnoses__age_at_diagnosis {
              stats {
                min
                max
              }
              histogram(interval: $interval) {
                buckets {
                  doc_count
                  key
                }
              }
            }
          }
        }
        cohort2: cases {
          hits(filters: $cohort2) {
            total
          }
          facets(filters: $cohort2, facets: $facets)
          aggregations(filters: $cohort2) {
            diagnoses__age_at_diagnosis {
              stats {
                min
                max
              }
              histogram(interval: $interval) {
                buckets {
                  doc_count
                  key
                }
              }
            }
          }
        }
      }
    }
  }
  """

GDC_MMRFComparison_GQL_Query_Variables = {
    "cohort1": {
        "op": "and",
        "content": [
            {
                "op": "in",
                "content": {
                    "field": "cases.case_id",
                    "value": ["set_id:3oHICax9M7e-k8hdqGdzDA"],
                },
            }
        ],
    },
    "cohort2": {
        "op": "and",
        "content": [
            {
                "op": "in",
                "content": {
                    "field": "cases.case_id",
                    "value": ["set_id:HYF-9YpiUHuiFjqARprIaQ"],
                },
            }
        ],
    },
    "facets": [
        "demographic.ethnicity",
        "demographic.gender",
        "demographic.race",
        "demographic.vital_status",
        "diagnoses.age_at_diagnosis",
    ],
    "interval": 3652.5,
}


async def get_facets():
    data = await gdc_client.execute(
        query=GDC_MMRFComparison_GQL_Query,
        variables=GDC_MMRFComparison_GQL_Query_Variables,
    )
    if data.get("error"):
        raise InternalError(data.get("error"))
    dataRoot = data.get("data", {}).get("viewer", {}).get("explore", {})
    return dataRoot


if __name__ == "__main__":
    print("Running in debug mode")
    data = asyncio.run(get_facets())
    print_json(data=data)
