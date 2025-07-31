POST_COORDINATES = {
    "$schema": "http://json-schema.org/schema#",
    "type": "object",
    "properties": {
        "bai": {
            "type": "string",
        },
        "regions": {
            "type": "array",
            "items": {
                "type": "string",
                # This matches: HPV-mCG2, chrUn_JTFH01001989v1_decoy, chr7, chr3:2-55
                "pattern": "^[-_a-zA-Z0-9]+(:([0-9]+)?(-[0-9]+)?)?$",
            },
        },
        "order": {
            "type": "array",
            "items": {"type": "string"},
        },
        "unmapped": {"type": "boolean"},
        "tar": {"type": "boolean"},
    },
}

GET_COORDINATES = {
    "$schema": "http://json-schema.org/hyper-schema#",
    "title": "BAM Coordinate-slicing",
    "description": "Coordinate-slice for one or more BAMs.",
    "method": "POST",
    "rel": "create",
    "schema": POST_COORDINATES,
    "mediaType": "application/octet-stream",
}
