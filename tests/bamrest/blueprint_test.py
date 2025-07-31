from typing import Optional

from flask import Flask, json, request
from pytest import raises
from werkzeug.datastructures import MultiDict

from bamrest import exceptions

try:
    from urllib import urlencode
except ImportError:
    from urllib.parse import urlencode

import pytest

from bamrest.blueprint import (
    multi_dict_to_dict,
    parse_coordinates,
    read_params,
    to_list,
)
from bamrest.exceptions import BaseUserError


def test_multi_dict_to_dict_1():
    foo = MultiDict([("a", 1), ("a", 2)])

    assert multi_dict_to_dict(foo) == {"a": [1, 2]}


def test_multi_dict_to_dict_2():
    foo = MultiDict([("a", 1), ("b", 1), ("a", 2)])

    assert multi_dict_to_dict(foo) == {"a": [1, 2], "b": 1}


def test_multi_dict_to_dict_3():
    foo = MultiDict([("a", 1), ("b", 1), ("a", 2), ("b", 3)])

    assert multi_dict_to_dict(foo) == {"a": [1, 2], "b": [1, 3]}


app = Flask(__name__)


def test_read_params_GET_with_one_region():
    key = "region"
    value = "chr7:140505783-140511649"
    url = f"/view/be75fb61-ce15-45ea-a776-b384d71a11bd?{key}={value}"

    with app.test_request_context(url):
        assert request.method == "GET"
        assert request.args == MultiDict([(key, value)])
        assert read_params(request) == {key: value}


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
            str(e.value)
            == "Content-Type header for POST must be 'application/json' or 'application/x-www-form-urlencoded'"
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
    form_data = urlencode([("regions", "chr7"), ("regions", "chr7:140505783-140511650")])

    with app.test_request_context(
        "/view/be75fb61-ce15-45ea-a776-b384d71a11bd",
        method="POST",
        content_type=content_type,
        data=form_data,
    ):
        assert request.method == "POST"
        assert content_type in request.headers.get("Content-Type", "").lower()
        assert read_params(request) == {"regions": ["chr7", "chr7:140505783-140511650"]}


def test_read_params_POST_via_form_submission_and_url_params():
    content_type = "application/x-www-form-urlencoded"

    form_data = urlencode([("regions", "chr22_KI270732v1_random"), ("regions", "HTLV-1")])
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
            "regions": ["chr22_KI270732v1_random", "HTLV-1"],
        }


def test_to_list():
    assert to_list("a") == ["a"]
    assert to_list(["a"]) == ["a"]


@pytest.mark.parametrize(
    ("reference", "begining", "end"),
    (
        ("chr7", 1, 2),
        ("chr4", 23456, None),
        ("chr22_KI270732v1_random", None, None),
        ("HTLV-1", None, None),
    ),
)
def test__parse_coordinates__gets_data(
    reference: str, begining: Optional[int], end: Optional[int]
) -> None:
    if begining and end:
        coord = f"{reference}:{begining}-{end}"
    elif begining:
        coord = f"{reference}:{begining}"
    else:
        coord = reference

    results = parse_coordinates(coord)

    assert results == (reference, begining, end)


@pytest.mark.parametrize(
    "coord",
    (
        "chr7:140505783-*",
        "chr22_KI270732v1_random:3-",
        "HT LV-1",
    ),
)
def test__parse_coordinates__raise_malformed_data(coord: str) -> None:
    with pytest.raises(exceptions.CoordinateParsingError):
        parse_coordinates(coord)


def test__parse_coordinates__illigal_range() -> None:
    with pytest.raises(exceptions.CoordinateRangeError):
        parse_coordinates("ch29:4634-20")
