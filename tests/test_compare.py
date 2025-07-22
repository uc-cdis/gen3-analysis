import pytest
from unittest.mock import MagicMock


def mock_guppy_data(app):
    async def mocked_guppy_data():
        data = {
            "data": {
                "cohort1": {
                    "case": {
                        "project_id": {
                            "histogram": [{"key": "MMRF-COMMPASS", "count": 995}]
                        }
                    }
                },
                "cohort2": {
                    "case": {
                        "project_id": {
                            "histogram": [{"key": "MMRF-COMMPASS", "count": 995}]
                        }
                    }
                },
            }
        }
        return data

    mocked_guppy_client = MagicMock()
    mocked_guppy_client.execute = lambda *args, **kwargs: (
        await mocked_guppy_data() for _ in "_"
    ).__anext__()
    app.state.guppy_client = mocked_guppy_client


@pytest.mark.asyncio
async def test_compare_endpoint(app, client):
    cohort1 = {
        "AND": [
            {
                "=": {
                    "project_id": "MMRF-COMMPASS",
                },
            },
            # {
            #     "=": {
            #         "gender": "male",
            #     },
            # },
        ],
    }
    cohort2 = {
        "AND": [
            {
                "=": {
                    "project_id": "MMRF-COMMPASS",
                },
            },
            # {
            #     "=": {
            #         "ethnicity": "not hispanic or latino",
            #     },
            # },
        ],
    }
    body = {
        "cohort1": cohort1,
        "cohort2": cohort2,
        "facets": [
            # "ethnicity",
            # "gender",
            # "race",
            # "vital_status",
            "project_id",  # TODO remove
            # "age_at_diagnosis",
        ],
        "interval": 3652.5,
    }

    mock_guppy_data(app)

    res = await client.post("/compare/facet", json=body)
    assert res.status_code == 200, res.json()

    import json; print(json.dumps(res.json(), indent=2))
