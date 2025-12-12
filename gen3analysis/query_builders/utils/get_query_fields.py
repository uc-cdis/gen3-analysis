from gen3analysis.utils.filterEdit import get_subfields
from gen3analysis.utils.group import build_fields_query_body


def get_query_fields(fields, expand, expandable_fields, default_fields):

    all_fields = []
    if not fields:
        all_fields.extend(default_fields)
    else:
        all_fields.extend(fields)

    if expand:
        for field in expand:
            expand_fields = get_subfields(expandable_fields, field)
            for f in expand_fields:
                all_fields.append(f)

    query_fields = build_fields_query_body(list(set(all_fields)))
    return query_fields
