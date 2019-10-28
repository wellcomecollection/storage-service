#!/usr/bin/env python
# -*- encoding: utf-8
"""
Given the JSON output of what_is_in_preservica.py, create an interactive
HTML view of the Preservica DB.

Usage:

    get_preservica_html_tree.py <COLLECTIONS_JSON>

"""

import json
import os
import sys

from jinja2 import Environment, FileSystemLoader


STATUSES = json.load(open("migration_status.json"))


def pprint_html_tree(tree, is_root=False, parent_is_deleted=False):
    lines = []

    entries = sorted(tree, key=lambda t: t["metadata"]["CollectionCode"])

    if is_root:
        lines.append("<ul class='collapsibleList'>")
    else:
        lines.append("<ul class='collapsibleList'>")

    for entry in entries:
        code = entry["metadata"]["CollectionCode"]
        title = entry["metadata"]["Title"]
        collection_ref = entry["metadata"]["CollectionRef"]

        status = STATUSES.get(collection_ref, {})

        to_delete = status.get("status") == "delete" or parent_is_deleted

        lines.append("<li>")

        if status.get("status") == "delete":
            lines.append("<div class='status__delete'>")
        elif status.get("status") == "ignore":
            lines.append("<div class='status__ignore'>")
        else:
            lines.append("<div>")

        if title != code:
            lines.append(f"{code}: {title}")
        else:
            lines.append(code)

        lines.append(f'<br/><span class="collection_ref">{collection_ref}</span>')

        if entry["children"]:
            lines.extend(
                "  " + l
                for l in pprint_html_tree(
                    entry["children"], is_root=False, parent_is_deleted=to_delete
                )
            )

        lines.append("</div>")
        lines.append("</li>")

    lines.append("</ul>")

    return lines


if __name__ == "__main__":
    try:
        path = sys.argv[1]
    except IndexError:
        sys.exit(f"Usage: {__file__} <COLLECTIONS_JSON>")

    data = json.load(open(path))

    env = Environment(loader=FileSystemLoader("."))
    template = env.get_template("tree_template.html")
    content = "\n".join(pprint_html_tree(data.values(), is_root=True))
    html_string = template.render(content=content)

    with open("tree.html", "w") as of:
        of.write(html_string)

    os.system("open tree.html")
