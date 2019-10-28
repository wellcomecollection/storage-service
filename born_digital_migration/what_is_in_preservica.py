#!/usr/bin/env python
# -*- encoding: utf-8
"""
This script dumps a JSON file with a copy of the tree in the Explorer view of
Preservica, i.e., what you see if you look at:

    http://sdb.wellcome.ac.uk/explorer/explorer.html

You need Preservica DB access to run this script.

"""

import json

import attr
import helpers


@attr.s
class Collection:
    metadata = attr.ib()
    children = attr.ib(factory=list)

    @property
    def id(self):
        return self.metadata["CollectionRef"]


class CollectionEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Collection):
            return {"metadata": obj.metadata, "children": obj.children}


if __name__ == "__main__":
    cnxn = helpers.get_connection()

    all_collections = {}

    for row in helpers.get_iterator(cnxn, query="SELECT * FROM Collection"):
        collection = Collection(metadata=row)
        all_collections[collection.id] = collection

    root_collections = {}

    for c_id, collection in all_collections.items():
        if not collection.metadata["ParentRef"]:
            root_collections[c_id] = collection
        else:
            all_collections[collection.metadata["ParentRef"]].children.append(
                collection
            )

    json_string = json.dumps(
        root_collections, cls=CollectionEncoder, indent=2, sort_keys=True
    )

    with open("collections.json", "w") as outfile:
        outfile.write(json_string)
