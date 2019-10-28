#!/usr/bin/env python
# -*- encoding: utf-8
"""
Given the JSON output of what_is_in_preservica.py, create a little tree view of
the Preservica database.

Usage:

    get_preservica_txt_tree.py <COLLECTIONS_JSON>

"""

import json
import sys


def pprint_nested_tree(tree, is_root=True):
    lines = []

    if is_root:
        lines.append(".")

    entries = sorted(tree, key=lambda t: t["metadata"]["CollectionCode"])

    for i, entry in enumerate(entries, start=1):
        code = entry["metadata"]["CollectionCode"]
        title = entry["metadata"]["Title"]
        collection_ref = entry["metadata"]["CollectionRef"]

        if i == len(entries):
            lines.append("│")
            lines.append("└── " + code)

            if title != code:
                lines.append("    " + title)

            lines.append("    " + collection_ref)
            lines.extend(
                [
                    "    " + l
                    for l in pprint_nested_tree(entry["children"], is_root=False)
                ]
            )

        else:
            lines.append("│")
            lines.append("├── " + code)

            if title != code:
                lines.append("│   " + title)
            lines.append("│   " + collection_ref)

            lines.extend(
                [
                    "│   " + l
                    for l in pprint_nested_tree(entry["children"], is_root=False)
                ]
            )

    return lines


if __name__ == "__main__":
    try:
        path = sys.argv[1]
    except IndexError:
        sys.exit(f"Usage: {__file__} <COLLECTIONS_JSON>")

    data = json.load(open(path))

    with open("tree.txt", "w") as of:
        of.write("\n".join(pprint_nested_tree(data.values())))
