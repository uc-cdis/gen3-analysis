from gen3analysis.filters.convertGDCGQLFiltersToFilters import (
    convert_gql_operation_to_operation,
)
import json
from dataclasses import dataclass, asdict

GDCFilters = [
    {
        "op": "and",
        "content": [
            {
                "op": "=",
                "content": {"field": "cases.project.project_id", "value": "TCGA-BRCA"},
            },
            {
                "op": "=",
                "content": {
                    "field": "gene.ssm.ssm_id",
                    "value": "edd1ae2c-3ca9-52bd-a124-b09ed304fcc2",
                },
            },
        ],
    },
    {
        "op": "and",
        "content": [
            {
                "op": "=",
                "content": {"field": "cases.project.project_id", "value": "TCGA-BRCA"},
            },
            {
                "op": "excludeifany",
                "content": {
                    "field": "gene.ssm.ssm_id",
                    "value": "edd1ae2c-3ca9-52bd-a124-b09ed304fcc2",
                },
            },
        ],
    },
]


if __name__ == "__main__":
    user_dict = convert_gql_operation_to_operation(GDCFilters[0])
    user_json = json.dumps(asdict(user_dict), indent=2)
    print(user_json)
    print(convert_gql_operation_to_operation(GDCFilters[0]))
