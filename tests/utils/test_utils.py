from gen3analysis.utils.group import build_fields_query_body


def test_group_paths_empty_list():
    """Test build_fields_query_body with an empty list."""
    paths = []
    expected = ""
    actual = build_fields_query_body(paths)
    assert actual == expected


def test_group_paths_single_root_leaf():
    """Test build_fields_query_body with a single 'root.leaf' style path."""
    paths = ["root.leaf"]
    expected = "root { leaf }"
    actual = build_fields_query_body(paths)
    assert actual == expected


def test_group_paths_multiple_roots():
    """Test build_fields_query_body with a single 'root.leaf' style path."""
    paths = ["root.leaf", "case", "file_id"]
    expected = "root { leaf } case file_id"
    actual = build_fields_query_body(paths)
    assert actual == expected


def test_group_paths_many_fields():
    """Test build_fields_query_body with a single 'root.leaf' style path."""
    paths = ["root.leaf", "case", "file_id", "project.program.name"]
    expected = "root { leaf } case file_id project { program { name } }"
    actual = build_fields_query_body(paths)
    assert actual == expected


def test_mix_leaf_types():
    """Test build_fields_query_body with a single 'root.leaf' style path."""
    paths = [
        "root.leaf",
        "case",
        "file_id",
        "project.program.name",
        "test.alpha",
        "test.alpha.leaf2",
        "test.alpha.leaf3",
    ]
    expected = "root { leaf } case file_id project { program { name },   }"
    actual = build_fields_query_body(paths)
    assert actual == expected
