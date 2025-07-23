import json

import pytest
from unittest.mock import MagicMock


cohort1 = {
    "AND": [{"=": {"project_id": "MMRF-COMMPASS"}}, {"=": {"vital_status": "alive"}}]
}
cohort2 = {
    "AND": [
        {"=": {"project_id": "MMRF-COMMPASS"}},
        {"=": {"ethnicity": "not hispanic or latino"}},
    ]
}


def mock_guppy_data(app, data):
    # bk = app.state.guppy_client

    async def mocked_guppy_data():
        return data

    mocked_guppy_client = MagicMock()
    mocked_guppy_client.execute = lambda *args, **kwargs: (
        await mocked_guppy_data() for _ in "_"
    ).__anext__()
    app.state.guppy_client = mocked_guppy_client

    # yield # TODO doesn't work
    # app.state.guppy_client = bk


@pytest.mark.asyncio
async def test_compare_facets_endpoint(app, client):
    mocked_guppy_data = {
        "data": {
            "cohort1": {
                "case": {
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
                                    {"key": "key1", "count": 4},
                                    {"key": "key2", "count": 7},
                                ]
                            }
                        }
                    },
                }
            },
            "cohort2": {
                "case": {
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
                                    {"key": "key1", "count": 66},
                                    {"key": "key2", "count": 21},
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
            "demographic.ethnicity",
            "abc.def.ghi",
            # "age_at_diagnosis",
        ],
        "interval": 3652.5,
    }
    res = await client.post("/compare/facets", json=body)
    assert res.status_code == 200, res.json()
    print("Result:", json.dumps(res.json(), indent=2))
    assert res.json() == {
        "cohort1": {
            "facets": {
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
    mocked_guppy_data = {
        "data": {
            "cohort1": {"case": {"_case_id": {"_cardinalityCount": 35}}},
            "cohort2": {"case": {"_case_id": {"_cardinalityCount": 44}}},
            "intersection": {"case": {"_case_id": {"_cardinalityCount": 9}}},
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
    print("Result:", json.dumps(res.json(), indent=2))
    assert res.json() == {"cohort1": 35, "cohort2": 44, "intersection": 9}
