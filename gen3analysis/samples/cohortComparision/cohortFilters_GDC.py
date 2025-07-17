Cohort1_GDC = {
    "op": "and",
    "content": [
        {
            "op": "in",
            "content": {
                "field": "cases.project.project_id",
                "value": ["MMRF-COMMPASS"],
            },
        },
        {
            "op": "in",
            "content": {"field": "cases.demographic.gender", "value": ["male"]},
        },
    ],
}
Cohort2_GDC = (
    {
        "op": "and",
        "content": [
            {
                "op": "in",
                "content": {
                    "field": "cases.project.project_id",
                    "value": ["MMRF-COMMPASS"],
                },
            },
            {
                "op": "in",
                "content": {
                    "field": "cases.demographic.ethnicity",
                    "value": ["not hispanic or latino"],
                },
            },
        ],
    },
)
