"""Helper functions for creating flask Response objects."""

import datetime
import json
import logging
from markupsafe import Markup

logger = logging.getLogger(__name__)


def is_pretty(options):
    return options.get("pretty", "false").lower() == "true"


def to_json(options, data):
    return (
        json.dumps(data, indent=2, separators=(", ", ": "))
        if is_pretty(options)
        else json.dumps(data)
    )


def format_response(request_options, data, mimetype):
    """
    Returns data as a response with the format specified either as a parameter (priority)
    or as a Accept header in the request.

    Args:
        request_options (dict): args or form fields from Request object
        data (dict or string): data to be formatted and returned in the Response body
        mimetype (string)

    Returns:
        A Flask Response object, with the data formatted as specified and the Content-Type set
    """

    def get_response(data, mimetype):
        response = Response(data, mimetype=mimetype)
        return response

    is_attachment = request_options.get("attachment", "")
    is_attachment = is_attachment is True or is_attachment.lower() == "true"

    if isinstance(data, str):
        return get_response(data, mimetype)

    if isinstance(data, dict) and (
        is_attachment
        or "text/csv" in mimetype
        or "text/tab-separated-values" in mimetype
    ):
        if "hits" in data["data"]:
            data = data["data"]["hits"]
        else:
            data = [data["data"]]

    if isinstance(data, dict):
        pagination = data.get("data", {}).get("pagination", None)
        if pagination:
            data["data"]["pagination"] = striptags_from_dict(pagination)
        warnings = data.get("warnings", None)
        if warnings:
            data["warnings"] = striptags_from_dict(warnings)

    else:
        mimetype = "application/json"
        data = to_json(request_options, data)

    return get_response(data, mimetype)
