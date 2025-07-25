import json

import pytest
from unittest.mock import MagicMock


project_id = "test-project-id"
cohort1 = {
    "AND": [
        {"=": {"project_id": project_id}},
        {"nested": {"path": "demographic", "=": {"vital_status": "Alive"}}},
    ]
}
cohort2 = {
    "AND": [
        {"=": {"project_id": project_id}},
        {
            "nested": {
                "path": "demographic",
                "=": {"ethnicity": "hispanic or latino"},
            }
        },
    ]
}


def mock_guppy_data(app, data):
    async def mocked_guppy_data():
        return data

    mocked_guppy_client = MagicMock()
    # making this function a MagicMock allows us to use methods like
    # `assert_called_once_with` in the tests
    mocked_execute_function = MagicMock(
        side_effect=lambda *args, **kwargs: (
            await mocked_guppy_data() for _ in "_"
        ).__anext__()
    )
    mocked_guppy_client.execute = mocked_execute_function
    app.state.guppy_client = mocked_guppy_client


@pytest.mark.asyncio
async def test_compare_facets_endpoint(app, client):
    mocked_guppy_data = {
        "data": {
            "cohort1": {
                "case": {
                    "project_id": {"histogram": [{"key": project_id, "count": 144}]},
                    "demographic": {
                        "ethnicity": {
                            "histogram": [
                                {"key": "hispanic or latino", "count": 99},
                                {"key": "not hispanic or latino", "count": 45},
                            ]
                        }
                    },
                    "abc": {
                        "def": {
                            "ghi": {
                                "histogram": [
                                    {"key": "key1", "count": 100},
                                    {"key": "key2", "count": 44},
                                ]
                            }
                        }
                    },
                }
            },
            "cohort2": {
                "case": {
                    "project_id": {"histogram": [{"key": project_id, "count": 144}]},
                    "demographic": {
                        "ethnicity": {
                            "histogram": [
                                {"key": "hispanic or latino", "count": 0},
                                {"key": "not hispanic or latino", "count": 30},
                            ]
                        }
                    },
                    "abc": {
                        "def": {
                            "ghi": {
                                "histogram": [
                                    {"key": "key1", "count": 26},
                                    {"key": "key2", "count": 4},
                                ]
                            }
                        }
                    },
                }
            },
        }
    }
    mock_guppy_data(app, mocked_guppy_data)

    body = {
        "doc_type": "case",
        "cohort1": cohort1,
        "cohort2": cohort2,
        "facets": [
            "project_id",  # not nested
            "demographic.ethnicity",  # nested
            "abc.def.ghi",  # doubly nested
            # "age_at_diagnosis",  # TODO numeric histogram
        ],
        "interval": 3652.5,
    }
    res = await client.post("/compare/facets", json=body)
    assert res.status_code == 200, res.json()

    app.state.guppy_client.execute.assert_called_once_with(
        query="query ($cohort1: JSON, $cohort2: JSON){\n        cohort1: _aggregation {\n            case (filter: $cohort1) { project_id { histogram { key count } } demographic { ethnicity { histogram { key count } } } abc { def { ghi { histogram { key count } } } }  }\n        }\n        cohort2: _aggregation {\n            case (filter: $cohort2) { project_id { histogram { key count } } demographic { ethnicity { histogram { key count } } } abc { def { ghi { histogram { key count } } } }  }\n        }\n    }",
        variables={"cohort1": cohort1, "cohort2": cohort2},
    )

    print("Result:", json.dumps(res.json(), indent=2))
    assert res.json() == {
        "cohort1": {
            "facets": {
                "project_id": {
                    "buckets": mocked_guppy_data["data"]["cohort1"]["case"][
                        "project_id"
                    ]["histogram"]
                },
                "demographic.ethnicity": {
                    "buckets": mocked_guppy_data["data"]["cohort1"]["case"][
                        "demographic"
                    ]["ethnicity"]["histogram"]
                },
                "abc.def.ghi": {
                    "buckets": mocked_guppy_data["data"]["cohort1"]["case"]["abc"][
                        "def"
                    ]["ghi"]["histogram"]
                },
            }
        },
        "cohort2": {
            "facets": {
                "project_id": {
                    "buckets": mocked_guppy_data["data"]["cohort2"]["case"][
                        "project_id"
                    ]["histogram"]
                },
                "demographic.ethnicity": {
                    "buckets": mocked_guppy_data["data"]["cohort2"]["case"][
                        "demographic"
                    ]["ethnicity"]["histogram"]
                },
                "abc.def.ghi": {
                    "buckets": mocked_guppy_data["data"]["cohort2"]["case"]["abc"][
                        "def"
                    ]["ghi"]["histogram"]
                },
            }
        },
    }


@pytest.mark.asyncio
async def test_compare_intersection_endpoint(app, client):
    n_c1_ids = 35
    n_c2_ids = 44
    n_both_ids = 9
    mocked_guppy_data = {
        "data": {
            "cohort1": {"case": {"_case_id": {"_cardinalityCount": n_c1_ids}}},
            "cohort2": {"case": {"_case_id": {"_cardinalityCount": n_c2_ids}}},
            "intersection": {"case": {"_case_id": {"_cardinalityCount": n_both_ids}}},
        }
    }
    mock_guppy_data(app, mocked_guppy_data)

    body = {
        "doc_type": "case",
        "cohort1": cohort1,
        "cohort2": cohort2,
    }
    res = await client.post("/compare/intersection", json=body)
    assert res.status_code == 200, res.json()

    app.state.guppy_client.execute.assert_called_once_with(
        query="query ($cohort1: JSON, $cohort2: JSON, $intersection: JSON) {\n        cohort1: _aggregation {\n            case (filter: $cohort1) {\n                _case_id {\n                    _cardinalityCount(precision_threshold: 3000)\n                }\n            }\n        }\n        cohort2: _aggregation {\n            case (filter: $cohort2) {\n                _case_id {\n                    _cardinalityCount(precision_threshold: 3000)\n                }\n            }\n        }\n        intersection: _aggregation {\n            case (filter: $intersection) {\n                _case_id {\n                    _cardinalityCount(precision_threshold: 3000)\n                }\n            }\n        }\n    }",
        variables={
            "cohort1": cohort1,
            "cohort2": cohort2,
            "intersection": {"AND": [cohort1, cohort2]},
        },
    )

    print("Result:", json.dumps(res.json(), indent=2))
    assert res.json() == {
        "cohort1": n_c1_ids - n_both_ids,
        "cohort2": n_c2_ids - n_both_ids,
        "intersection": n_both_ids,
    }
