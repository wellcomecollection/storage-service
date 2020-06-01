# CHANGELOG

## v2.0.0 - 2020-06-01

This is a refactor of the internals: there's an abstract class `StorageServiceClientBase`, which can be extended with different HTTP libraries as required.
The library includes two implementations: `RequestsStorageServiceClient` and `RequestsOAuthStorageServiceClient`.

Some methods have also changed their signature: `space_id` is now `space`, `source_id` is now `external_identifier`.

Ingests are created with the new `amazon-s3` storage provider

## v1.5.0 - 2019-11-26

Allow specifying the top-level directory in an archive when downloading a
compressed bag, by passing `top_level_dir` to `download_compressed_bag`.

## v1.4.1 - 2019-11-14

Change to the way compressed bags are downloaded, so now the archive includes
directory entries.

## v1.4.0 - 2019-11-11

Improvements to the way compressed bags are downloaded, specifically:

-   The modified time inside the archive is set to the creation date of the storage manifest
-   Files are now compressed within a directory inside the archive, so unpacking the archive with `tar -xzf` will put all the files inside a directory

## v1.3.0 - 2019-10-23

Add two methods for downloading the complete contents of a bag to disk.  Namely:

-  `download_bag()`, which downloads the bag as unpacked files
-  `download_compressed_bag()`, which downloads the bag as a tar.gz file

## v1.2.1 - 2019-08-08

If the OAuth token for connecting to the storage service expires, the library refreshes the token rather than throwing a `TokenExpired` error.

## v1.2.0 - 2019-07-18

Add support for getting a bag by version.

## v1.1.0 - 2019-07-09

Add support for the `ingest_type` and `external_identifier` parameters when creating an ingest.

## v1.0.0 - 2019-04-02

Initial version!
