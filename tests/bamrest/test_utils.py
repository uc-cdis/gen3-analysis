from werkzeug.datastructures import MultiDict

from bamrest.blueprint import multi_dict_to_dict, to_list

# MultiDict


def test_multi_dict_to_dict_1():
    foo = MultiDict([("a", 1), ("a", 2)])

    assert multi_dict_to_dict(foo) == {"a": [1, 2]}


def test_multi_dict_to_dict_2():
    foo = MultiDict([("a", 1), ("b", 1), ("a", 2)])

    assert multi_dict_to_dict(foo) == {"a": [1, 2], "b": 1}


def test_multi_dict_to_dict_3():
    foo = MultiDict([("a", 1), ("b", 1), ("a", 2), ("b", 3)])

    assert multi_dict_to_dict(foo) == {"a": [1, 2], "b": [1, 3]}


# to list


def test_to_list():
    assert to_list("a") == ["a"]
    assert to_list(["a"]) == ["a"]
