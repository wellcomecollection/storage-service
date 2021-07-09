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

-   **The demo doesn't have any monitoring.**

    We have proactive monitoring to warn us about issues in the storage service, like:

    -   A daily report in Slack telling us how many bags were stored, and if any failed unexpectedly or have stalled
    -   Alarms in Slack for DLQ failures

    This monitoring isn't provided as part of the demo.

-   **The demo only keeps your files in a single geographic region and cloud provider.**

    Ideally, you should keep your files spread across:

    -   Multiple geographic regions
    -   Multiple cloud storage providers

    To keep the demo simple, it only creates two copies of your files in Amazon S3, both in the same region.
    The storage service also supports replicating to Azure Blob in a different region, but this is not enabled in the demo.
