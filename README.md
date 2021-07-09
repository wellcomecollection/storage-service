# storage-service

[![Build status](https://badge.buildkite.com/844e7fa8968b4cea276557cd9886753395d159dc2823eb8249.svg?branch=main)](https://buildkite.com/wellcomecollection/storage-service)

This is the Wellcome Collection storage service.
It manages the storage of our digital collections, including:

*   Uploading files to cloud storage providers like Amazon S3 and Azure Blob
*   Verifying fixity information on our files (checksums, sizes, filenames)
*   Reporting on the contents of our digital archive through machine-readable APIs and search tools



## Requirements

The storage service is designed to:

-   Ensure the safe, long-term (i.e. decades) storage of our digital assets
-   Provide a scalable mechanism for identifying, retrieving, and storing content
-   Follow industry best-practices around file integrity and audit trails
-   Enable us to meet [NDSA Level 4][ndsa] for both digitised and ["born-digital"][born_digital] assets

[ndsa]: https://ndsa.org/activities/levels-of-digital-preservation/
[born_digital]: https://en.wikipedia.org/wiki/Born-digital



## High-level design

<img src="docs/images/high_level_design.svg">

The user uploads a "bag" to the storage service.
This bag should use the [BagIt packaging format][bagit].
The user could be a person, of an automated workflow system like [Goobi](https://www.intranda.com/en/digiverso/goobi/goobi-overview/) or [Archivematica](https://archivematica.org/).

The storage service verifies the fixity information in the bag (checksums, file sizes, filenames).
If the fixity information is correct, it replicates the bag to multiple storage locations, split across different cloud providers and geographic locations.

The storage service stores exactly the bytes you give it; no more, no less.
It does not do any introspection of the bag contents, or change its behaviour based on the files a bag contains.

The storage service runs entirely in AWS, with no on-premise infrastructure required.

For more detailed information about the design, see [our documentation](docs).

[bagit]: https://datatracker.ietf.org/doc/html/rfc8493



## Usage

We run two instances of the storage service at Wellcome:

*   A ["prod" environment][prod] that holds our real collections
*   A ["staging" environment][staging] that we use for testing and development

Each instance of the storage service is completely separate.
They don't share any files or storage.

If you want to store files in the storage service, you should run your own instance -- the instances we run are only for use at Wellcome.
We publish our Docker images and infrastructure code, to allow other people to run the storage service.

For instructions, see [our documentation](docs).

### Getting started: use Terraform and AWS to run a storage service demo

We have [a Terraform configuration](demo/terraform) that spins up an instance of the storage service.
You can use this to try the storage service in your own AWS account.

[prod]: https://en.wikipedia.org/wiki/Deployment_environment#Production
[staging]: https://en.wikipedia.org/wiki/Deployment_environment#Staging



## License

MIT.
