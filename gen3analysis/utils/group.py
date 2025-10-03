from collections import defaultdict


def group_paths(paths: list[str]) -> dict[str, list[str]]:
    result = defaultdict(list)
    for path in paths:
        parts = path.split(".")
        if len(parts) == 2:  # only handle "root.leaf"
            root, leaf = parts
            result[root].append(leaf)
        elif len(parts) > 2:  # deeper nesting if needed
            root, rest = parts[0], ".".join(parts[1:])
            result[root].append(rest)
        else:
            result["_root"].append(parts[0])  # strings without "."
    # deduplicate
    return {k: sorted(set(v)) for k, v in result.items()}
