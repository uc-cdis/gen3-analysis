import pytest

from conftest import TEST_ACCESS_TOKEN, TEST_PROJECT_ID
from tests.utils import mock_guppy_data

mocked_guppy_data = [
    {
        "data": {
            "_aggregation": {"case": {"_totalCount": 6}},
            "case": [
                {
                    "_case_id": "a88a74e1-4a3f-44e9",
                    "demographic": [{"days_to_death": 769, "vital_status": "Dead"}],
                    "diagnoses": [{"days_to_last_follow_up": 769}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_1392",
                },
                {
                    "_case_id": "4e123b99-32aa-4ef2",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 1007}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_1980",
                },
                {
                    "_case_id": "07f4512a-7188-4db1",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 1467}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_1325",
                },
                {
                    "_case_id": "abcbdf03-a7e9-412f",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 876}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_2265",
                },
                {
                    "_case_id": "39b84ffe-fee5-48aa",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 734}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_2377",
                },
                {
                    "_case_id": "6912c587-3794-440a",
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
                    "_case_id": "2856f371-6acd-4f41",
                    "demographic": [{"days_to_death": 575, "vital_status": "Dead"}],
                    "diagnoses": [{"days_to_last_follow_up": 575}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_1960",
                },
                {
                    "_case_id": "fd8aec27-5a3a-4388",
                    "demographic": [{"days_to_death": 574, "vital_status": "Dead"}],
                    "diagnoses": [{"days_to_last_follow_up": 574}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_1771",
                },
                {
                    "_case_id": "a6a339e4-e0f8-41e8",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 1352}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_1510",
                },
                {
                    "_case_id": "0ef3738f-d41f-4769",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 1031}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_1391",
                },
                {
                    "_case_id": "fe1747dd-1557-40ca",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 1537}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_1324",
                },
                {
                    "_case_id": "e3ea3c76-ab6d-4904",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 58}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_1648",
                },
                {
                    "_case_id": "40afdf8c-8845-4e81",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 1786}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_1033",
                },
                {
                    "_case_id": "dcf3dd2a-6266-479d",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 144}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_2203",
                },
                {
                    "_case_id": "fccf2b8d-7864-44d5",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 1287}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_1512",
                },
                {
                    "_case_id": "244ceae2-0616-4daf",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 1080}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_1785",
                },
                {
                    "_case_id": "19feea32-2a89-4f76",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 1457}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_1293",
                },
                {
                    "_case_id": "2405e871-f922-43ed",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 1157}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_1772",
                },
                {
                    "_case_id": "98fb9c36-110c-4f38",
                    "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                    "diagnoses": [{"days_to_last_follow_up": 1065}],
                    "project_id": TEST_PROJECT_ID,
                    "submitter_id": "ID_1845",
                },
                {
                    "_case_id": "b496dcb2-e5e1-42f0",
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
                    "id": "6912c587-3794-440a",
                    "submitter_id": "ID_1456",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 1.0,
                    "censored": True,
                },
                {
                    "time": 734,
                    "id": "39b84ffe-fee5-48aa",
                    "submitter_id": "ID_2377",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 1.0,
                    "censored": True,
                },
                {
                    "time": 769,
                    "id": "a88a74e1-4a3f-44e9",
                    "submitter_id": "ID_1392",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 1.0,
                    "censored": False,
                },
                {
                    "time": 876,
                    "id": "abcbdf03-a7e9-412f",
                    "submitter_id": "ID_2265",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 0.7500000000000001,
                    "censored": True,
                },
                {
                    "time": 1007,
                    "id": "4e123b99-32aa-4ef2",
                    "submitter_id": "ID_1980",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 0.7500000000000001,
                    "censored": True,
                },
                {
                    "time": 1467,
                    "id": "07f4512a-7188-4db1",
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
                    "id": "e3ea3c76-ab6d-4904",
                    "submitter_id": "ID_1648",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 1.0,
                    "censored": True,
                },
                {
                    "time": 144,
                    "id": "dcf3dd2a-6266-479d",
                    "submitter_id": "ID_2203",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 1.0,
                    "censored": True,
                },
                {
                    "time": 574,
                    "id": "fd8aec27-5a3a-4388",
                    "submitter_id": "ID_1771",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 1.0,
                    "censored": False,
                },
                {
                    "time": 575,
                    "id": "2856f371-6acd-4f41",
                    "submitter_id": "ID_1960",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 0.9166666666666667,
                    "censored": False,
                },
                {
                    "time": 766,
                    "id": "b496dcb2-e5e1-42f0",
                    "submitter_id": "ID_2437",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 0.8333333333333335,
                    "censored": True,
                },
                {
                    "time": 1031,
                    "id": "0ef3738f-d41f-4769",
                    "submitter_id": "ID_1391",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 0.8333333333333335,
                    "censored": True,
                },
                {
                    "time": 1065,
                    "id": "98fb9c36-110c-4f38",
                    "submitter_id": "ID_1845",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 0.8333333333333335,
                    "censored": True,
                },
                {
                    "time": 1080,
                    "id": "244ceae2-0616-4daf",
                    "submitter_id": "ID_1785",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 0.8333333333333335,
                    "censored": True,
                },
                {
                    "time": 1157,
                    "id": "2405e871-f922-43ed",
                    "submitter_id": "ID_1772",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 0.8333333333333335,
                    "censored": True,
                },
                {
                    "time": 1287,
                    "id": "fccf2b8d-7864-44d5",
                    "submitter_id": "ID_1512",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 0.8333333333333335,
                    "censored": True,
                },
                {
                    "time": 1352,
                    "id": "a6a339e4-e0f8-41e8",
                    "submitter_id": "ID_1510",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 0.8333333333333335,
                    "censored": True,
                },
                {
                    "time": 1457,
                    "id": "19feea32-2a89-4f76",
                    "submitter_id": "ID_1293",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 0.8333333333333335,
                    "censored": True,
                },
                {
                    "time": 1537,
                    "id": "fe1747dd-1557-40ca",
                    "submitter_id": "ID_1324",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 0.8333333333333335,
                    "censored": True,
                },
                {
                    "time": 1786,
                    "id": "40afdf8c-8845-4e81",
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


compare_response = {
    "results": [
        {
            "meta": {"id": 4726943616},
            "donors": [
                {
                    "time": 157,
                    "id": "9eb1131c-e62a-46ee",
                    "submitter_id": "ID_2079",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 1,
                    "censored": True,
                },
                {
                    "time": 169,
                    "id": "1a353aaa-65eb-4aba",
                    "submitter_id": "ID_2787",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 1,
                    "censored": True,
                },
                {
                    "time": 180,
                    "id": "57791483-300c-40c2",
                    "submitter_id": "ID_2808",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 1,
                    "censored": True,
                },
                {
                    "time": 203,
                    "id": "9f9df522-7540-4c50",
                    "submitter_id": "ID_2801",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 1,
                    "censored": True,
                },
                {
                    "time": 328,
                    "id": "d602cbbb-6ccd-443e",
                    "submitter_id": "ID_1939",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 1,
                    "censored": False,
                },
                {
                    "time": 413,
                    "id": "4193e0cf-6b39-4f0e",
                    "submitter_id": "ID_2702",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 0.8333333333333333,
                    "censored": True,
                },
                {
                    "time": 452,
                    "id": "dfcd198b-4330-4e47",
                    "submitter_id": "ID_2679",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 0.8333333333333333,
                    "censored": True,
                },
                {
                    "time": 484,
                    "id": "af40a85d-745f-410c",
                    "submitter_id": "ID_2653",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 0.8333333333333333,
                    "censored": True,
                },
                {
                    "time": 966,
                    "id": "87c88a50-68c1-4c85",
                    "submitter_id": "ID_2058",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 0.8333333333333333,
                    "censored": True,
                },
                {
                    "time": 1261,
                    "id": "40cc4089-1b2b-497b",
                    "submitter_id": "ID_1602",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 0.8333333333333333,
                    "censored": True,
                },
            ],
        },
        {
            "meta": {"id": 4730409600},
            "donors": [
                {
                    "time": 444,
                    "id": "4a2316e9-8690-4d1e",
                    "submitter_id": "ID_1131",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 1,
                    "censored": True,
                },
                {
                    "time": 773,
                    "id": "57036b4d-0b40-4c80",
                    "submitter_id": "ID_2339",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 1,
                    "censored": True,
                },
                {
                    "time": 845,
                    "id": "36dd9339-06d5-4c09",
                    "submitter_id": "ID_2267",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 1,
                    "censored": True,
                },
                {
                    "time": 918,
                    "id": "18ac233c-a8e7-4157",
                    "submitter_id": "ID_1880",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 1,
                    "censored": True,
                },
                {
                    "time": 918,
                    "id": "6380c8fa-d32f-4a75",
                    "submitter_id": "ID_2056",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 1,
                    "censored": True,
                },
                {
                    "time": 995,
                    "id": "053d0f37-5ffa-4bfd",
                    "submitter_id": "ID_1988",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 1,
                    "censored": True,
                },
                {
                    "time": 1080,
                    "id": "f25a2b8d-05eb-4665",
                    "submitter_id": "ID_1796",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 1,
                    "censored": True,
                },
                {
                    "time": 1082,
                    "id": "9f5664d5-dc66-427e",
                    "submitter_id": "ID_1841",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 1,
                    "censored": True,
                },
                {
                    "time": 1098,
                    "id": "38aec8eb-aca4-4c2a",
                    "submitter_id": "ID_1744",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 1,
                    "censored": True,
                },
                {
                    "time": 1632,
                    "id": "f0a82e98-5d4d-4493",
                    "submitter_id": "ID_1184",
                    "project_id": TEST_PROJECT_ID,
                    "survivalEstimate": 1,
                    "censored": True,
                },
            ],
        },
    ],
    "overallStats": {"pValue": 0.1967056024589432, "degreesFreedom": 1},
}

cohort_a = {
    "data": {
        "case": [
            {"_case_id": "4504a259-fa2d-4907"},
            {"_case_id": "3fbe5718-860b-424d"},
            {"_case_id": "6a227cb7-4b49-49e5"},
            {"_case_id": "e4edadb6-6cde-421b"},
            {"_case_id": "1bad0750-e557-4465"},
            {"_case_id": "827f83de-2978-4e1f"},
            {"_case_id": "de556316-0012-4845"},
            {"_case_id": "691dfd51-29d0-40b0"},
            {"_case_id": "f954ad83-f285-42e7"},
            {"_case_id": "588926f0-67b5-4ee0"},
            {"_case_id": "53f271e5-42bf-43dc"},
            {"_case_id": "e7e8ead2-f02d-40f0"},
            {"_case_id": "c60a78c9-936a-4016"},
            {"_case_id": "7924d094-9aec-4623"},
            {"_case_id": "70d1ba40-794e-45e6"},
            {"_case_id": "a6a339e4-e0f8-41e8"},
            {"_case_id": "5acdf538-23a2-450b"},
            {"_case_id": "8fe0a8dd-849a-4062"},
            {"_case_id": "60b3c672-4466-470f"},
            {"_case_id": "e8446228-bc5a-4ade"},
            {"_case_id": "bbd9fc9a-3f04-43f5"},
            {"_case_id": "a84bee86-24ce-4ab6"},
            {"_case_id": "b54a526f-2ce1-41ea"},
            {"_case_id": "2b71f6f8-6dcc-40b2"},
        ]
    }
}

cohort_b = {
    "data": {
        "case": [
            {"_case_id": "4504a259-fa2d-4907"},
            {"_case_id": "3fbe5718-860b-424d"},
            {"_case_id": "57036b4d-0b40-4c80"},
            {"_case_id": "6a227cb7-4b49-49e5"},
            {"_case_id": "1bad0750-e557-4465"},
            {"_case_id": "4c616f0b-d7f7-4790"},
            {"_case_id": "a3e1e9c6-29af-43bd"},
            {"_case_id": "827f83de-2978-4e1f"},
            {"_case_id": "9ba5997d-5dea-4448"},
            {"_case_id": "691dfd51-29d0-40b0"},
            {"_case_id": "f954ad83-f285-42e7"},
            {"_case_id": "53f271e5-42bf-43dc"},
            {"_case_id": "e7e8ead2-f02d-40f0"},
            {"_case_id": "f4538c9c-b897-4b43"},
            {"_case_id": "c60a78c9-936a-4016"},
            {"_case_id": "70d1ba40-794e-45e6"},
            {"_case_id": "5acdf538-23a2-450b"},
            {"_case_id": "8fe0a8dd-849a-4062"},
            {"_case_id": "0732a04c-d9db-41ba"},
            {"_case_id": "afd30d80-6d45-403b"},
        ]
    }
}

result_cohort_a = {
    "data": {
        "_aggregation": {"case": {"_totalCount": 10}},
        "case": [
            {
                "_case_id": "d602cbbb-6ccd-443e",
                "demographic": [{"days_to_death": 328, "vital_status": "Dead"}],
                "diagnoses": [{"days_to_last_follow_up": 328}],
                "project_id": TEST_PROJECT_ID,
                "submitter_id": "ID_1939",
            },
            {
                "_case_id": "9eb1131c-e62a-46ee",
                "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                "diagnoses": [{"days_to_last_follow_up": 157}],
                "project_id": TEST_PROJECT_ID,
                "submitter_id": "ID_2079",
            },
            {
                "_case_id": "1a353aaa-65eb-4aba",
                "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                "diagnoses": [{"days_to_last_follow_up": 169}],
                "project_id": TEST_PROJECT_ID,
                "submitter_id": "ID_2787",
            },
            {
                "_case_id": "9f9df522-7540-4c50",
                "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                "diagnoses": [{"days_to_last_follow_up": 203}],
                "project_id": TEST_PROJECT_ID,
                "submitter_id": "ID_2801",
            },
            {
                "_case_id": "dfcd198b-4330-4e47",
                "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                "diagnoses": [{"days_to_last_follow_up": 452}],
                "project_id": TEST_PROJECT_ID,
                "submitter_id": "ID_2679",
            },
            {
                "_case_id": "4193e0cf-6b39-4f0e",
                "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                "diagnoses": [{"days_to_last_follow_up": 413}],
                "project_id": TEST_PROJECT_ID,
                "submitter_id": "ID_2702",
            },
            {
                "_case_id": "40cc4089-1b2b-497b",
                "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                "diagnoses": [{"days_to_last_follow_up": 1261}],
                "project_id": TEST_PROJECT_ID,
                "submitter_id": "ID_1602",
            },
            {
                "_case_id": "af40a85d-745f-410c",
                "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                "diagnoses": [{"days_to_last_follow_up": 484}],
                "project_id": TEST_PROJECT_ID,
                "submitter_id": "ID_2653",
            },
            {
                "_case_id": "87c88a50-68c1-4c85",
                "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                "diagnoses": [{"days_to_last_follow_up": 966}],
                "project_id": TEST_PROJECT_ID,
                "submitter_id": "ID_2058",
            },
            {
                "_case_id": "57791483-300c-40c2",
                "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                "diagnoses": [{"days_to_last_follow_up": 180}],
                "project_id": TEST_PROJECT_ID,
                "submitter_id": "ID_2808",
            },
        ],
    }
}

result_cohort_b = {
    "data": {
        "_aggregation": {"case": {"_totalCount": 10}},
        "case": [
            {
                "_case_id": "57036b4d-0b40-4c80",
                "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                "diagnoses": [{"days_to_last_follow_up": 773}],
                "project_id": TEST_PROJECT_ID,
                "submitter_id": "ID_2339",
            },
            {
                "_case_id": "053d0f37-5ffa-4bfd",
                "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                "diagnoses": [{"days_to_last_follow_up": 995}],
                "project_id": TEST_PROJECT_ID,
                "submitter_id": "ID_1988",
            },
            {
                "_case_id": "f25a2b8d-05eb-4665",
                "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                "diagnoses": [{"days_to_last_follow_up": 1080}],
                "project_id": TEST_PROJECT_ID,
                "submitter_id": "ID_1796",
            },
            {
                "_case_id": "18ac233c-a8e7-4157",
                "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                "diagnoses": [{"days_to_last_follow_up": 918}],
                "project_id": TEST_PROJECT_ID,
                "submitter_id": "ID_1880",
            },
            {
                "_case_id": "36dd9339-06d5-4c09",
                "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                "diagnoses": [{"days_to_last_follow_up": 845}],
                "project_id": TEST_PROJECT_ID,
                "submitter_id": "ID_2267",
            },
            {
                "_case_id": "6380c8fa-d32f-4a75",
                "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                "diagnoses": [{"days_to_last_follow_up": 918}],
                "project_id": TEST_PROJECT_ID,
                "submitter_id": "ID_2056",
            },
            {
                "_case_id": "f0a82e98-5d4d-4493",
                "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                "diagnoses": [{"days_to_last_follow_up": 1632}],
                "project_id": TEST_PROJECT_ID,
                "submitter_id": "ID_1184",
            },
            {
                "_case_id": "4a2316e9-8690-4d1e",
                "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                "diagnoses": [{"days_to_last_follow_up": 444}],
                "project_id": TEST_PROJECT_ID,
                "submitter_id": "ID_1131",
            },
            {
                "_case_id": "9f5664d5-dc66-427e",
                "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                "diagnoses": [{"days_to_last_follow_up": 1082}],
                "project_id": TEST_PROJECT_ID,
                "submitter_id": "ID_1841",
            },
            {
                "_case_id": "38aec8eb-aca4-4c2a",
                "demographic": [{"days_to_death": None, "vital_status": "Alive"}],
                "diagnoses": [{"days_to_last_follow_up": 1098}],
                "project_id": TEST_PROJECT_ID,
                "submitter_id": "ID_1744",
            },
        ],
    }
}

mocked_compare_guppy_data = [
    cohort_a,
    cohort_b,
    result_cohort_a,
    result_cohort_b,
]


@pytest.mark.asyncio
async def test_survival_compare_endpoint(app, client):
    parameters = {
        "filters": [
            {"nested": {"path": "demographic", "in": {"gender": ["male"]}}},
            {"nested": {"path": "demographic", "in": {"race": ["white"]}}},
        ],
        "doc_type": "case",
        "field": "_case_id",
    }
    mock_guppy_data(app, mocked_compare_guppy_data)

    res = await client.post(
        "/survival/compare",
        json=parameters,
        headers={"Authorization": f"bearer {TEST_ACCESS_TOKEN}"},
    )
    assert res.status_code == 200
    result_json = res.json()
    assert len(result_json["results"]) == 2
    assert (
        result_json["results"][0]["donors"] == compare_response["results"][0]["donors"]
    )
    assert (
        result_json["results"][1]["donors"] == compare_response["results"][1]["donors"]
    )
    assert result_json["overallStats"] == compare_response["overallStats"]
