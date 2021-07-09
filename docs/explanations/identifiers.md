# How identifiers work in the storage service

Bags in the storage service have a three-part identifier:

*   **Space:** the broad category of a bag.
    Examples: `digitised`, `born-digital`.

*   **External identifier:** the identifier of a bag within a space.
    This is typically an identifier from another system, which matches this bag to that record.
    Examples: `b31497652`, `PP/CRI/A/2`.

*   **Version:** an auto-incrementing numeric value.
    This tracks distinct versions of a (space, external identifier) pair.
    Examples: `v1`, `v2`, `v3`.

The space and external identifier are supplied by the user; the version is automatically generated by the storage service.

These three parts can be combined into a single string, which uniquely identifies a bag; for example `digitised/b31497652/v2`.
This identifier is also the path to the root of the bag inside our storage buckets.

Why did we choose this approach?

-   We want identifiers that are human-readable and understandable.
    (As opposed to, say, [UUIDs][uuids].)

-   We match bags to records in systems outside the storage service (for example, the library catalogue).
    This approach allows us to use the same identifier as the external system, rather than [inventing another type of identifier][standards].

-   This structure allows us to group related content by space within the underlying storage, in a way that is human-readable:

    ```
    digitised/
      ├── record1/
      │     ├── v1/
      │     │    ├── bagit.txt
      │     │    ├── bag-info.txt
      │     │    └── data/
      │     │          ├── record1_0001.jp2
      │     │          └── ...
      │     └── v2/
      │          └── ...
      │
      └── record2/
            └── v1/
                 └── ...
    ```

    The human-readable storage layout means our files are not tied to the specific software implementation of the storage service.

[uuids]: https://en.wikipedia.org/wiki/Universally_unique_identifier
[standards]: https://xkcd.com/927/