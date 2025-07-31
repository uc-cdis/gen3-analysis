import pytest

from conftest import TEST_ACCESS_TOKEN, TEST_PROJECT_ID
from tests.utils import mock_guppy_data

mocked_guppy_data = [
    {
        "data": {
            "_aggregation": {"case": {"_totalCount": 6}},
            "case": [
                {
                    "_case_id": "a88a74e1-4a3f-44e9-a521-7ba612cd8935",
                    "demographic": [{"days_to_death": 769, "vital_status": "Dead"}],
                    "diagnoses": [{"days_to_last_follow_up": 769}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_1392",
                },
                {
                    "_case_id": "4e123b99-32aa-4ef2-a709-edeb8b9e48a1",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 1007}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_1980",
                },
                {
                    "_case_id": "07f4512a-7188-4db1-8571-fd74564dd3f0",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 1467}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_1325",
                },
                {
                    "_case_id": "abcbdf03-a7e9-412f-9edd-4ffb93436adb",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 876}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_2265",
                },
                {
                    "_case_id": "39b84ffe-fee5-48aa-b244-6c27aab4248c",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 734}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_2377",
                },
                {
                    "_case_id": "6912c587-3794-440a-ac3f-620869010d93",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 92}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_1456",
                },
            ],
        }
    },
    {
        "data": {
            "_aggregation": {"case": {"_totalCount": 14}},
            "case": [
                {
                    "_case_id": "2856f371-6acd-4f41-b633-de4a756ab7ef",
                    "demographic": [{"days_to_death": 575, "vital_status": "Dead"}],
                    "diagnoses": [{"days_to_last_follow_up": 575}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_1960",
                },
                {
                    "_case_id": "fd8aec27-5a3a-4388-b63f-1f912a5b34e9",
                    "demographic": [{"days_to_death": 574, "vital_status": "Dead"}],
                    "diagnoses": [{"days_to_last_follow_up": 574}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_1771",
                },
                {
                    "_case_id": "a6a339e4-e0f8-41e8-abb8-93ac949d0cb0",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 1352}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_1510",
                },
                {
                    "_case_id": "0ef3738f-d41f-4769-b9ef-792e3945588e",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 1031}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_1391",
                },
                {
                    "_case_id": "fe1747dd-1557-40ca-80e3-0e6a31f0fc7b",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 1537}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_1324",
                },
                {
                    "_case_id": "e3ea3c76-ab6d-4904-8edb-f3a35ab34d94",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 58}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_1648",
                },
                {
                    "_case_id": "40afdf8c-8845-4e81-ad54-2872e5a230f5",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 1786}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_1033",
                },
                {
                    "_case_id": "dcf3dd2a-6266-479d-8aec-6b2bd40b4e6c",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 144}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_2203",
                },
                {
                    "_case_id": "fccf2b8d-7864-44d5-b3e8-17ca3474d88b",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 1287}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_1512",
                },
                {
                    "_case_id": "244ceae2-0616-4daf-8b2a-e2335f185e6a",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 1080}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_1785",
                },
                {
                    "_case_id": "19feea32-2a89-4f76-b707-10db96e5fab4",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 1457}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_1293",
                },
                {
                    "_case_id": "2405e871-f922-43ed-a3e0-236b212e23b8",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 1157}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_1772",
                },
                {
                    "_case_id": "98fb9c36-110c-4f38-b4ea-6d9f4169b419",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 1065}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_1845",
                },
                {
                    "_case_id": "b496dcb2-e5e1-42f0-8ed9-776a59db6e5a",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 766}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_2437",
                },
            ],
        }
    },
]

survival_response = {
    "results": [
        {
            "meta": {"id": 4881527232},
            "donors": [
                {
                    "time": 92,
                    "id": "6912c587-3794-440a-ac3f-620869010d93",
                    "submitter_id": "ID_1456",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 1.0,
                    "censored": True,
                },
                {
                    "time": 734,
                    "id": "39b84ffe-fee5-48aa-b244-6c27aab4248c",
                    "submitter_id": "ID_2377",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 1.0,
                    "censored": True,
                },
                {
                    "time": 769,
                    "id": "a88a74e1-4a3f-44e9-a521-7ba612cd8935",
                    "submitter_id": "ID_1392",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 1.0,
                    "censored": False,
                },
                {
                    "time": 876,
                    "id": "abcbdf03-a7e9-412f-9edd-4ffb93436adb",
                    "submitter_id": "ID_2265",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 0.7500000000000001,
                    "censored": True,
                },
                {
                    "time": 1007,
                    "id": "4e123b99-32aa-4ef2-a709-edeb8b9e48a1",
                    "submitter_id": "ID_1980",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 0.7500000000000001,
                    "censored": True,
                },
                {
                    "time": 1467,
                    "id": "07f4512a-7188-4db1-8571-fd74564dd3f0",
                    "submitter_id": "ID_1325",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 0.7500000000000001,
                    "censored": True,
                },
            ],
        },
        {
            "meta": {"id": 4653085248},
            "donors": [
                {
                    "time": 58,
                    "id": "e3ea3c76-ab6d-4904-8edb-f3a35ab34d94",
                    "submitter_id": "ID_1648",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 1.0,
                    "censored": True,
                },
                {
                    "time": 144,
                    "id": "dcf3dd2a-6266-479d-8aec-6b2bd40b4e6c",
                    "submitter_id": "ID_2203",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 1.0,
                    "censored": True,
                },
                {
                    "time": 574,
                    "id": "fd8aec27-5a3a-4388-b63f-1f912a5b34e9",
                    "submitter_id": "ID_1771",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 1.0,
                    "censored": False,
                },
                {
                    "time": 575,
                    "id": "2856f371-6acd-4f41-b633-de4a756ab7ef",
                    "submitter_id": "ID_1960",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 0.9166666666666667,
                    "censored": False,
                },
                {
                    "time": 766,
                    "id": "b496dcb2-e5e1-42f0-8ed9-776a59db6e5a",
                    "submitter_id": "ID_2437",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 0.8333333333333335,
                    "censored": True,
                },
                {
                    "time": 1031,
                    "id": "0ef3738f-d41f-4769-b9ef-792e3945588e",
                    "submitter_id": "ID_1391",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 0.8333333333333335,
                    "censored": True,
                },
                {
                    "time": 1065,
                    "id": "98fb9c36-110c-4f38-b4ea-6d9f4169b419",
                    "submitter_id": "ID_1845",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 0.8333333333333335,
                    "censored": True,
                },
                {
                    "time": 1080,
                    "id": "244ceae2-0616-4daf-8b2a-e2335f185e6a",
                    "submitter_id": "ID_1785",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 0.8333333333333335,
                    "censored": True,
                },
                {
                    "time": 1157,
                    "id": "2405e871-f922-43ed-a3e0-236b212e23b8",
                    "submitter_id": "ID_1772",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 0.8333333333333335,
                    "censored": True,
                },
                {
                    "time": 1287,
                    "id": "fccf2b8d-7864-44d5-b3e8-17ca3474d88b",
                    "submitter_id": "ID_1512",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 0.8333333333333335,
                    "censored": True,
                },
                {
                    "time": 1352,
                    "id": "a6a339e4-e0f8-41e8-abb8-93ac949d0cb0",
                    "submitter_id": "ID_1510",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 0.8333333333333335,
                    "censored": True,
                },
                {
                    "time": 1457,
                    "id": "19feea32-2a89-4f76-b707-10db96e5fab4",
                    "submitter_id": "ID_1293",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 0.8333333333333335,
                    "censored": True,
                },
                {
                    "time": 1537,
                    "id": "fe1747dd-1557-40ca-80e3-0e6a31f0fc7b",
                    "submitter_id": "ID_1324",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 0.8333333333333335,
                    "censored": True,
                },
                {
                    "time": 1786,
                    "id": "40afdf8c-8845-4e81-ad54-2872e5a230f5",
                    "submitter_id": "ID_1033",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 0.8333333333333335,
                    "censored": True,
                },
            ],
        },
    ],
    "overallStats": {"pValue": 0.9143976212093845, "degreesFreedom": 1},
}


@pytest.mark.asyncio
async def test_survival_endpoint(app, client):
    filters = {
        "filters": [
            {"and": [{"nested": {"path": "demographic", "in": {"race": ["other"]}}}]},
            {"and": [{"nested": {"path": "demographic", "in": {"race": ["asian"]}}}]},
        ]
    }

    mock_guppy_data(app, mocked_guppy_data)

    res = await client.post(
        "/survival/",
        json=filters,
        headers={"Authorization": f"bearer {TEST_ACCESS_TOKEN}"},
    )
    assert res.status_code == 200
    result_json = res.json()
    assert len(result_json["results"]) == 2
    assert (
        result_json["results"][0]["donors"] == survival_response["results"][0]["donors"]
    )
    assert (
        result_json["results"][1]["donors"] == survival_response["results"][1]["donors"]
    )
    assert result_json["overallStats"] == survival_response["overallStats"]
