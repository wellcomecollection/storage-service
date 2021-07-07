# Limitations of the Terraform demo

This document explains some of the limitations of [our Terraform demo](../README.md#creating-the-demo).

The demo is meant for testing the storage service, not to be a production-ready environment for real data.

-   **Resources are all managed in a single configuration.**

    We recommend managing persistent resources (e.g. permanent S3 buckets) in a separate configuration from ephemeral resources (e.g. applications).
    This reduces the risk of a bad application change affecting the permanent data stores.

-   **There are no access controls on the S3 buckets.**

    In a real environment, you should use tools like IAM policies and S3 Object Lock to restrict who can access your permanent storage buckets.
    These are not configured on the demo buckets.

-   **The logging/reporting cluster is underprovisioned for a real instance.**

    The Elasticsearch cluster only has a single node, and the same cluster is used for both application logs and storage service reporting.
    Additionally, you need to configure index lifecycle policies on the logging cluster, or the disk will fill up.

-   **The application versioning needs some work.**

    Currently the services will pull the latest version of the storage service that's available.
    This is sub-optimal for a variety of reasons.

    If non-Wellcome organisations want to use the storage service for real data, then we (Wellcome) need to come up with a better way to deliver software and updates.
