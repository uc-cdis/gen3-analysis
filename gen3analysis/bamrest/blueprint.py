import functools
import logging
import re
from datetime import datetime
from itertools import chain

import flask
import jsonschema

from . import exceptions, schemas, slicing

blueprint = flask.Blueprint("bamrest", __name__)
blueprint.config = dict()


# private helpers
def multi_dict_to_dict(multi_dict):
    """
    Form fields or GET args can be repeated for multi-value fields.
    When MultiDict object is casted to dict, it's converted to list
    for all values. Here we unbox the value for lists of one single
    element and let the ones with multiple values remain as lists.
    Args:
        multi_dict (Multidict): args or form fields from Request object
    Returns:
        a dict after this 'flattening/unboxing' transformation
    """
    return {key: value if len(value) > 1 else value[0] for key, value in multi_dict.lists()}


def read_post_params(request):
    """
    Read and gather parameters from a POST request, either from a
    POST JSON payload or from form fields.
    Args:
        request (Request): Request object
    Returns:
        options (dict): args or form fields from Request object
    """
    content_type = request.headers.get("Content-Type", "").lower()

    if "application/x-www-form-urlencoded" in content_type:
        data = multi_dict_to_dict(request.form)
    elif "application/json" in content_type:
        if request.data == "":
            return {}
        else:
            data = request.json

    else:
        raise exceptions.BaseUserError(
            "Content-Type header for POST must be 'application/json' "
            "or 'application/x-www-form-urlencoded'"
        )

    return data


def read_params(request):
    """
    Read and gather parameters from a POST request, combining the JSON
    payload or form fields with parameters from the URL.
    Args:
        request (Request): Request object
    Returns:
        options (dict): args or form fields from Request object
    """
    params = multi_dict_to_dict(request.args)

    if request.method == "POST":
        params.update(read_post_params(request))

    return params


def remove_download_token_from_cookie(params, response):
    """
    Removes a download token in cookie as an indicator that download is ready.
    Args:
        params (dict): args or form fields from Request object
        response: Response object
    Returns:
        The response object that is passed in
    """
    cookie_key = params.get("downloadCookieKey", "")
    cookie_path = params.get("downloadCookiePath", "/")

    if cookie_key != "":
        response.set_cookie(cookie_key, expires=0, path=cookie_path)

    return response


def customize_attachment_headers(params, bam_id, response):
    """
    If the browser/user requests for an attachment (a download), we
    add the appropriate HTTP headers so the browser can prompt the
    user to save the download.
    Args:
        params (dict): args or form fields from Request object
        bam_id (string): ID of the BAM file to slice
        response: Response object
    Returns:
        The response object that is passed in
    """
    is_attachment = params.get("attachment", False)
    is_attachment = (
        is_attachment if isinstance(is_attachment, bool) else is_attachment.lower() == "true"
    )

    if is_attachment:
        file_name = "{name}.{timestamp}.sliced.bam".format(
            name=bam_id, timestamp=datetime.now().isoformat()
        )
        response.headers.add("Content-Disposition", "attachment", filename=file_name)
        response = remove_download_token_from_cookie(params, response)

    return response


def to_list(a):
    return a if isinstance(a, list) else [a]


def parse_request():
    """
    Parse request params from args, form and json payload
    and validate with jsonschema
    """
    params = read_params(flask.request)

    params["regions"] = list(
        chain.from_iterable(
            [to_list(params.get("region", [])), to_list(params.get("regions", []))]
        )
    )

    params["gencode"] = to_list(params.get("gencode", []))
    jsonschema.validate(params, schemas.POST_COORDINATES)
    return params


@blueprint.route("/view/<bam>", methods=["GET", "POST"])
def get_coordinates_slices_for_bam(bam):
    """
    Retrieves alignments from a specified BAM file.
    """
    try:
        get = blueprint.get
    except AttributeError as err:
        raise NotImplementedError(err)

    try:
        bam2bai = blueprint.bam2bai
    except AttributeError as err:
        raise NotImplementedError(err)

    try:
        gencode_to_regions = blueprint.gencode_to_regions
    except AttributeError as err:
        raise NotImplementedError(err)

    params = parse_request()

    # parse regions
    regions = []
    get_unmapped = False
    for r in params["regions"]:
        if r == "unmapped":
            get_unmapped = True
        else:
            parsed_region = parse_coordinates(r)
            regions.append(parsed_region)

    # Convert gencode gene names to regions.
    for g in params["gencode"]:
        regions.extend(gencode_to_regions(g))

    bai = params.get("bai", bam2bai(bam))

    slices = slicing.coordinate(
        functools.partial(get, bam), functools.partial(get, bai), regions, get_unmapped
    )

    return customize_attachment_headers(
        params, bam, flask.Response(flask.stream_with_context(slices))
    )


def parse_coordinates(coord):
    """
    Parses chromosomal coordinates.
    """
    # TODO this should be moved to a config file
    REGEX = r"^(?P<ref>[-\w]+)(?::(?P<beg>\d+)(?:-(?P<end>\d+))?)?$"
    regex = re.compile(REGEX)
    match = regex.match(coord)

    if match is None:
        err = f"malformed coordinate range: {coord}"
        raise exceptions.CoordinateParsingError(err)

    match = match.groupdict()

    ref = match.get("ref")
    beg = match.get("beg")
    end = match.get("end")

    beg = int(beg) if beg is not None else beg
    end = int(end) if end is not None else end

    if beg is not None and end is not None and end < beg:
        err = f"invalid range: {coord}"
        raise exceptions.CoordinateRangeError(err)

    return ref, beg, end


@blueprint.errorhandler(jsonschema.exceptions.ValidationError)
def handle_schema_error(err):
    """
    Handle jsonschema errors.
    """
    return flask.jsonify(error=err.message, message=err.message), 400


@blueprint.errorhandler(exceptions.BaseUserError)
def handle_user_error(err):
    """
    Handle user errors.
    """
    return flask.jsonify(error=str(err), message=str(err)), 400


@blueprint.errorhandler(exceptions.BasePermissionError)
def handle_permission_error(err):
    """
    Handle permission errors.
    """
    return flask.jsonify(error=str(err), message=str(err)), 403


@blueprint.errorhandler(exceptions.BaseLookupError)
def handle_lookup_error(err):
    """
    Handle lookup errors.
    """
    return flask.jsonify(error=str(err), message=str(err)), 404


@blueprint.errorhandler(exceptions.BaseRetrievalError)
def handle_retrieval_error(err):
    """
    Handle retrieval errors.
    """
    return flask.jsonify(error=str(err), message=str(err)), 404


@blueprint.errorhandler(exceptions.BaseSlicingError)
def handle_slicing_error(err):
    """
    Handle early slicing errors.
    """
    # NOTE This only occurs when the slicing error happens early. Specifically,
    # before the streaming response has started. This implies that there is
    # some form of data corruption early-on in the file.
    logging.error("slicing error encountered")
    logging.exception(err)
    return (
        flask.jsonify(
            error="unexpected error during slicing", message="unexpected error during slicing"
        ),
        503,
    )


@blueprint.record
def get_config(setup_state):
    """
    Get configuration options for the blueprint via the app config.
    """
    blueprint.config.update(setup_state.app.config.get("BAMREST", dict()))
