def build_tree(paths):
    """
    Converts a list of dot-delimited strings into a nested dictionary tree structure.

    Args:
        paths: List of strings with '.' as delimiter (e.g., ['a.b.c', 'a.b.d', 'a.e'])

    Returns:
        A nested dictionary where each level contains lists of children

    Example:
        >>> build_tree(['a.b.c', 'a.b.d', 'a.e', 'f'])
        {'a': {'b': ['c', 'd'], 'e': []}, 'f': []}
    """
    tree = {}

    for path in paths:
        parts = path.split(".")
        current = tree

        # Navigate through all parts except the last one
        for i, part in enumerate(parts[:-1]):
            # If this part doesn't exist, create it as a dict
            if part not in current:
                current[part] = {}
            # If it exists but is a list (leaf node), convert to dict
            elif isinstance(current[part], list):
                current[part] = {}
            current = current[part]

        # Handle the last part (leaf)
        last_part = parts[-1]
        if last_part not in current:
            current[last_part] = []
        # If it already exists as a dict, keep it as dict (has children)
        elif isinstance(current[last_part], list):
            pass  # Already a leaf, keep as an empty list

    return tree


def group_to_graphql(tree) -> str:
    """
    Converts a nested dictionary tree structure into a GraphQL field query string.

    Args:
        tree: Nested dictionary where lists represent leaf nodes and dicts represent branches
        indent_level: Current indentation level (used for recursion)

    Returns:
        A GraphQL query string
    """
    fields = []

    for key, value in tree.items():
        if isinstance(value, dict) and value:
            # This is a branch node with children
            nested_query = group_to_graphql(value)
            fields.append(f"{key} {{ {nested_query} }}")
        elif isinstance(value, dict) and not value:
            # Empty dict - treat as leaf
            fields.append(key)
        else:
            # This is a leaf node (empty list or other)
            fields.append(key)

    return " ".join(fields)


def build_fields_query_body(fields: list[str]) -> str:
    return group_to_graphql(build_tree(fields))
