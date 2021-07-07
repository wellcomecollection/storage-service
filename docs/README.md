# Storage service

The unit of storage in the storage service is a **bag**.
This is a collection of files packaged together with [the BagIt packaging format][bagit], which are ingested and stored together.

An **ingest** is a request to store a bag in the storage service.
It may succeed (if the bag is copied to permanent storage) or fail (if the bag is rejected, say for fixity errors).

Each bag is identified with a **space** (a broad category) an **external identifier** (a specific identifier) and a **version**.
[Read more about identifiers](explanations/identifiers.md).

[bagit]: https://datatracker.ietf.org/doc/html/rfc8493

## Tutorials


## How-to

-   [Ingest a bag into the storage service](howto/ingest-a-bag.md)
-   [Look up an already-stored bag in the storage service](howto/look-up-a-bag.md)
-   [Look up the versions of a bag in the storage service](howto/look-up-versions-of-a-bag.md)

## Reference
## Explanation
