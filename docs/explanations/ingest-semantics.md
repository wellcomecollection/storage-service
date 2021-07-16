# The semantics of bags, ingests and ingest types

This page describes the semantics of the storage service concepts, which may help you understand the design of the storage service.

A **bag** is the conceptual entity, which is manifested by individual **bag versions**.
All the files in a given space/external identifier form a single bag, with individual versions v1, v2, v3, and so on.

(Note: we also sometimes say "bag" to refer to a bag version -- i.e., a collection of files packaged with the BagIt packaging format.)

An **ingest** is a record of processing activity on a bag.
A bag can be associated with multiple ingests, e.g. one ingest for each bag version.
Ingests are immutable once complete, and if a bag undergoes further processing, we should create a new ingest.

The **ingest type** tells you what sort of processing this ingest is recording.
Currently it takes two values:

- `create` – store a brand new bag
-	`update` – add an additional manifestation/version of an already existing bag

This is designed to allow for future extension, for example adding operations like:

-	`delete` – remove a bag or a single manifestation of a bag
-	`replicate` – create an additional replica of a bag in a new storage provider

Neither of these operations are currently supported, but we may support them in the future.
