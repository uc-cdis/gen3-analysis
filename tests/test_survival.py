import json

import pytest
from unittest.mock import MagicMock

from conftest import TEST_ACCESS_TOKEN

mocked_guppy_data = {
    "data": {
        "_aggregation": {"case": {"_totalCount": 6}},
        "case": [
            {
                "_case_id": "a88a74e1-4a3f-44e9-a521-7ba612cd8935",
                "demographic": [{"days_to_death": 769, "vital_status": "Dead"}],
                "diagnoses": [{"days_to_last_follow_up": 769}],
                "project_id": "MMRF-COMMPASS",
                "submitter_id": "MMRF_1392",
            },
            {
                "_case_id": "4e123b99-32aa-4ef2-a709-edeb8b9e48a1",
                "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                "diagnoses": [{"days_to_last_follow_up": 1007}],
                "project_id": "MMRF-COMMPASS",
                "submitter_id": "MMRF_1980",
            },
            {
                "_case_id": "07f4512a-7188-4db1-8571-fd74564dd3f0",
                "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                "diagnoses": [{"days_to_last_follow_up": 1467}],
                "project_id": "MMRF-COMMPASS",
                "submitter_id": "MMRF_1325",
            },
            {
                "_case_id": "abcbdf03-a7e9-412f-9edd-4ffb93436adb",
                "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                "diagnoses": [{"days_to_last_follow_up": 876}],
                "project_id": "MMRF-COMMPASS",
                "submitter_id": "MMRF_2265",
            },
            {
                "_case_id": "39b84ffe-fee5-48aa-b244-6c27aab4248c",
                "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                "diagnoses": [{"days_to_last_follow_up": 734}],
                "project_id": "MMRF-COMMPASS",
                "submitter_id": "MMRF_2377",
            },
            {
                "_case_id": "6912c587-3794-440a-ac3f-620869010d93",
                "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                "diagnoses": [{"days_to_last_follow_up": 92}],
                "project_id": "MMRF-COMMPASS",
                "submitter_id": "MMRF_1456",
            },
        ],
    }
}

survival_response = {
    "results": [
        {
            "meta": {"id": 4881527232},
            "donors": [
                {
                    "time": 92,
                    "id": "6912c587-3794-440a-ac3f-620869010d93",
                    "submitter_id": "MMRF_1456",
                    "project_id": "MMRF-COMMPASS",
                    "survivalEstimate": 1.0,
                    "censored": True,
                },
                {
                    "time": 734,
                    "id": "39b84ffe-fee5-48aa-b244-6c27aab4248c",
                    "submitter_id": "MMRF_2377",
                    "project_id": "MMRF-COMMPASS",
                    "survivalEstimate": 1.0,
                    "censored": True,
                },
                {
                    "time": 769,
                    "id": "a88a74e1-4a3f-44e9-a521-7ba612cd8935",
                    "submitter_id": "MMRF_1392",
                    "project_id": "MMRF-COMMPASS",
                    "survivalEstimate": 0.7500000000000001,
                    "censored": False,
                },
                {
                    "time": 876,
                    "id": "abcbdf03-a7e9-412f-9edd-4ffb93436adb",
                    "submitter_id": "MMRF_2265",
                    "project_id": "MMRF-COMMPASS",
                    "survivalEstimate": 0.7500000000000001,
                    "censored": True,
                },
                {
                    "time": 1007,
                    "id": "4e123b99-32aa-4ef2-a709-edeb8b9e48a1",
                    "submitter_id": "MMRF_1980",
                    "project_id": "MMRF-COMMPASS",
                    "survivalEstimate": 0.7500000000000001,
                    "censored": True,
                },
                {
                    "time": 1467,
                    "id": "07f4512a-7188-4db1-8571-fd74564dd3f0",
                    "submitter_id": "MMRF_1325",
                    "project_id": "MMRF-COMMPASS",
                    "survivalEstimate": 0.7500000000000001,
                    "censored": True,
                },
            ],
        }
    ],
    "overallStats": {},
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
async def test_survival_endpoint(app, client):
    filters = {
        "filters": [
            {"and": [{"nested": {"path": "demographic", "in": {"race": ["other"]}}}]}
        ]
    }

    mock_guppy_data(app, mocked_guppy_data)

    res = await client.post(
        "/survival/",
        json=filters,
        headers={"Authorization": f"bearer {TEST_ACCESS_TOKEN}"},
    )
    assert res.status_code == 200
    assert (
        res.json()["results"][0]["donors"] == survival_response["results"][0]["donors"]
    )
