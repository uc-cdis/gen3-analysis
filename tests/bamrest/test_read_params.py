import pytest
from flask import json, request
from pytest import raises
from werkzeug.datastructures import MultiDict

try:
    from urllib import urlencode
except ImportError:
    from urllib.parse import urlencode

from bamrest.blueprint import read_params
from bamrest.exceptions import BaseUserError
from tests.conftest import app


@pytest.mark.parametrize(
    "region", ("chr7:140505783-140511649", "chr7", "chr22_KI270732v1_random", "HTLV-1")
)
def test_read_params_GET_with_one_region(region: str) -> None:
    key = "region"
    url = f"/view/be75fb61-ce15-45ea-a776-b384d71a11bd?{key}={region}"

    with app.test_request_context(url):
        assert request.method == "GET"
        assert request.args == MultiDict([(key, region)])
        assert read_params(request) == {key: region}


def test_read_params_GET_with_two_regions():
    key = "region"
    value1 = "chr7:140505783-140511649"
    value2 = "chr7:140505783-140511650"
    url = "/view/be75fb61-ce15-45ea-a776-b384d71a11bd?{key}={value1}&{key}={value2}".format(
        key=key, value1=value1, value2=value2
    )

    with app.test_request_context(url):
        assert request.method == "GET"
        assert request.args == MultiDict([(key, value1), (key, value2)])
        assert read_params(request) == {key: [value1, value2]}


def test_read_params_POST_with_unsupported_content_type():
    content_type = "text/plain"

    with app.test_request_context(
        "/view/be75fb61-ce15-45ea-a776-b384d71a11bd", method="POST", content_type=content_type
    ):
        assert request.method == "POST"

        with raises(BaseUserError) as e:
            read_params(request)

        assert (
            str(e.value) == "Content-Type header for POST must be "
            "'application/json' or 'application/x-www-form-urlencoded'"
        )


def test_read_params_POST_with_one_region():
    key = "regions"
    value = "chr7:140505783-140511649"
    content_type = "application/json"
    payload = json.dumps({key: [value]})

    with app.test_request_context(
        "/view/be75fb61-ce15-45ea-a776-b384d71a11bd",
        method="POST",
        content_type=content_type,
        data=payload,
    ):
        assert request.method == "POST"
        assert content_type in request.headers.get("Content-Type", "").lower()
        assert read_params(request) == {key: [value]}


def test_read_params_POST_with_both_json_and_url_params():
    content_type = "application/json"

    key1 = "region"
    value1 = "chr7:140505783-140511649"
    key2 = "regions"
    value2 = "chr7:140505783-140511650"

    url = f"/view/be75fb61-ce15-45ea-a776-b384d71a11bd?{key1}={value1}"
    payload = json.dumps({key2: [value2]})

    with app.test_request_context(url, method="POST", content_type=content_type, data=payload):
        assert request.method == "POST"
        assert read_params(request) == {key1: value1, key2: [value2]}


def test_read_params_POST_via_form_submission():
    content_type = "application/x-www-form-urlencoded"
    form_data = urlencode(
        [("regions", "chr7:140505783-140511649"), ("regions", "chr7:140505783-140511650")]
    )

    with app.test_request_context(
        "/view/be75fb61-ce15-45ea-a776-b384d71a11bd",
        method="POST",
        content_type=content_type,
        data=form_data,
    ):
        assert request.method == "POST"
        assert content_type in request.headers.get("Content-Type", "").lower()
        assert read_params(request) == {
            "regions": ["chr7:140505783-140511649", "chr7:140505783-140511650"]
        }


def test_read_params_POST_via_form_submission_and_url_params():
    content_type = "application/x-www-form-urlencoded"

    form_data = urlencode(
        [("regions", "chr7:140505783-140511649"), ("regions", "chr7:140505783-140511650")]
    )
    url = "/view/be75fb61-ce15-45ea-a776-b384d71a11bd?{key1}={value1}&{key2}={value2}".format(
        key1="region",
        value1="chr7:140505783-140511651",
        key2="region",
        value2="chr7:140505783-140511652",
    )

    with app.test_request_context(url, method="POST", content_type=content_type, data=form_data):
        assert request.method == "POST"
        assert content_type in request.headers.get("Content-Type", "").lower()
        assert read_params(request) == {
            "region": ["chr7:140505783-140511651", "chr7:140505783-140511652"],
            "regions": ["chr7:140505783-140511649", "chr7:140505783-140511650"],
        }
