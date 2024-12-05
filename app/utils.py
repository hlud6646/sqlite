from collections import defaultdict


def combine_dicts(dict_list):
    """Given an iterable of dicts, with all lists as values,
    combine into one dict by joning the lists."""
    result = defaultdict(list)
    for d in dict_list:
        for key, values in d.items():
            result[key].extend(values)
    return dict(result)
