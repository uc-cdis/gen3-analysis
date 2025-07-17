import asyncio
from gen3analysis.gdc.graphqlQuery import GDCGQLClient

filters = {
    "op": "and",
    "content": {
        "op": "or",
        "content": [
            {
                "op": "and",
                "content": [
                    {
                        "op": ">",
                        "content": {
                            "field": "demographic.days_to_death",
                            "value": 0,
                        },
                    },
                ],
            },
            {
                "op": "and",
                "content": [
                    {
                        "op": ">",
                        "content": {
                            "field": "diagnoses.days_to_last_follow_up",
                            "value": 0,
                        },
                    },
                ],
            },
        ],
    },
}
fields = "diagnoses.days_to_last_follow_up,demographic.days_to_death,demographic.vital_status,case_id,submitter_id,project.project_id"


def create_cases_query():
    return """query ClinicalAnalysisResult(
    $filters: FiltersArgument
    $facets: [String]!
  ) {
    viewer {
      explore {
        cases {
          facets(facets: $facets, filters: $filters)
        }
      }
    }
  }
  """


gdc_client = GDCGQLClient("https://portal.gdc.cancer.gov/auth/api/v0/graphql")

variables = {
    "filters": filters,
    "facets": fields,
}

query = """query QueryBucketCounts($filters: FiltersArgument) {
      viewer {
          explore {
            cases {
              aggregations(

                filters:$filters,
                aggregations_filter_themselves: false
              ) {
                 cases__demographic__gender : demographic__gender{buckets { doc_count key }}, cases__demographic__race : demographic__race{buckets { doc_count key }}, cases__demographic__ethnicity : demographic__ethnicity{buckets { doc_count key }}, cases__demographic__vital_status : demographic__vital_status{buckets { doc_count key }}
              }
            }
          }
        }
      }
  """

vars = {
    "filters": {
        "op": "and",
        "content": [
            {
                "op": "in",
                "content": {
                    "field": "cases.project.project_id",
                    "value": ["MMRF-COMMPASS"],
                },
            }
        ],
    }
}


async def main():
    #  result = await gdc_client.execute(query=create_cases_query(), variables=variables)
    result = await gdc_client.execute(query=query, variables=vars)
    print(result)


asyncio.run(main())
