import json

import pytest

from conftest import TEST_ACCESS_TOKEN, TEST_PROJECT_ID
from tests.utils import mock_guppy_data

cohort1 = {
    "AND": [
        {"=": {"project_id": TEST_PROJECT_ID}},
        {"nested": {"path": "demographic", "=": {"vital_status": "Alive"}}},
    ]
}
cohort2 = {
    "AND": [
        {"=": {"project_id": TEST_PROJECT_ID}},
        {
            "nested": {
                "path": "demographic",
                "=": {"ethnicity": "hispanic or latino"},
            }
        },
    ]
}


@pytest.mark.asyncio
async def test_compare_facets_endpoint(app, client):
    mocked_guppy_data = {
        "data": {
            "cohort1": {
                "case": {
                    "project_id": {
                        "histogram": [{"key": TEST_PROJECT_ID, "count": 144}]
                    },
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
                    "diagnoses": {
                        "age_at_diagnosis": {
                            "histogram": [
                                {
                                    "key": [7300, 10950],
                                    "count": 50,
                                },
                                {
                                    "key": [10950, 14600],
                                    "count": 94,
                                },
                            ]
                        }
                    },
                }
            },
            "cohort2": {
                "case": {
                    "project_id": {
                        "histogram": [{"key": TEST_PROJECT_ID, "count": 30}]
                    },
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
                    "diagnoses": {
                        "age_at_diagnosis": {
                            "histogram": [
                                {
                                    "key": [7320, 8950],
                                    "count": 30,
                                },
                            ]
                        }
                    },
                }
            },
        }
    }
    mock_guppy_data(app, [mocked_guppy_data])

    body = {
        "doc_type": "case",
        "cohort1": cohort1,
        "cohort2": cohort2,
        "facets": [
            "project_id",  # not nested
            "demographic.ethnicity",  # nested
            "abc.def.ghi",  # doubly nested
            "diagnoses.age_at_diagnosis",  # numeric histogram
        ],
        "interval": {"diagnoses.age_at_diagnosis": 3652},
    }
    res = await client.post(
        "/compare/facets",
        json=body,
        headers={"Authorization": f"bearer {TEST_ACCESS_TOKEN}"},
    )
    assert res.status_code == 200, res.json()

    app.state.guppy_client.execute.assert_called_once_with(
        access_token=TEST_ACCESS_TOKEN,
        query="query ($cohort1: JSON, $cohort2: JSON){\n        cohort1: _aggregation {\n            case (filter: $cohort1, accessibility: accessible) { project_id { histogram { key count } } demographic { ethnicity { histogram { key count } } } abc { def { ghi { histogram { key count } } } } diagnoses { age_at_diagnosis { histogram(rangeStep: 3652) { key count } } }  }\n        }\n        cohort2: _aggregation {\n            case (filter: $cohort2, accessibility: accessible) { project_id { histogram { key count } } demographic { ethnicity { histogram { key count } } } abc { def { ghi { histogram { key count } } } } diagnoses { age_at_diagnosis { histogram(rangeStep: 3652) { key count } } }  }\n        }\n    }",
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
                "diagnoses.age_at_diagnosis": {
                    "buckets": mocked_guppy_data["data"]["cohort1"]["case"][
                        "diagnoses"
                    ]["age_at_diagnosis"]["histogram"]
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
                "diagnoses.age_at_diagnosis": {
                    "buckets": mocked_guppy_data["data"]["cohort2"]["case"][
                        "diagnoses"
                    ]["age_at_diagnosis"]["histogram"]
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
    mock_guppy_data(app, [mocked_guppy_data])

    body = {
        "doc_type": "case",
        "cohort1": cohort1,
        "cohort2": cohort2,
    }
    res = await client.post(
        "/compare/intersection",
        json=body,
        headers={"Authorization": f"bearer {TEST_ACCESS_TOKEN}"},
    )
    assert res.status_code == 200, res.json()

    app.state.guppy_client.execute.assert_called_once_with(
        access_token=TEST_ACCESS_TOKEN,
        query="query ($cohort1: JSON, $cohort2: JSON, $intersection: JSON) {\n        cohort1: _aggregation {\n            case (filter: $cohort1, accessibility: accessible) {\n                _case_id {\n                    _cardinalityCount(precision_threshold: 3000)\n                }\n            }\n        }\n        cohort2: _aggregation {\n            case (filter: $cohort2, accessibility: accessible) {\n                _case_id {\n                    _cardinalityCount(precision_threshold: 3000)\n                }\n            }\n        }\n        intersection: _aggregation {\n            case (filter: $intersection, accessibility: accessible) {\n                _case_id {\n                    _cardinalityCount(precision_threshold: 3000)\n                }\n            }\n        }\n    }",
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
