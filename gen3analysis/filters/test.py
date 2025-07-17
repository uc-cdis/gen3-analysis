import json
from dataclasses import asdict

from gen3analysis.filters.convertFiltersToGen3GQLFilters import (
    convert_operation_to_gql,
)
from gen3analysis.filters.convertGDCGQLFiltersToFilters import (
    convert_gql_operation_to_operation,
)


GDCFilters = [
    {
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
    {"op": "not", "content": {"field": "demographic.vital_status"}},
]

if __name__ == "__main__":
    user_dict = convert_gql_operation_to_operation(GDCFilters[0])
    user_json = json.dumps(asdict(user_dict), indent=2)
    print("GDC Filter:")
    print(user_json)

    guppy_filters = convert_operation_to_gql(user_dict)
    # Use the dataclasses_json to_json method and then parse it back
    guppy_dict = guppy_filters.to_dict()
    # guppy_dict = json.loads(guppy_json_str)

    guppy_json = json.dumps(guppy_dict, indent=2)
    print("\nGen3 GQL Filter:")
    print(guppy_json)
