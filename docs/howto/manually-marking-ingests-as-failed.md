# Manually marking ingests as failed

Our monitoring tools will highlight ingests that have "stalled" – that is, that don't have any recent processing activity.
This is usually requires investigation, but once you've understood the cause you may want to manually mark the bag as failed.

e.g. We have ingests for test bags from early deployments of the storage service.
They never completed processing (because the storage service was still a work-in-progress), but we don't want them to be flagged as "stalled" for ever more.

This guide explains how to manually mark a bag as failed.

> **Note:** In the storage service, all updates to ingests are managed by the ingests tracker.
> You should also update ingests through the ingests tracker; do not modify the ingests database manually.
>
> This ensures all updates are applied consistently and propagate to other services (e.g. the ingests indexer).
>
> In the Wellcome instance of the storage service, it is not possible to manually modify the ingests database.

To manually mark a bag as failed:

1.  Identify the ID of the ingest you want to mark as failed.

2.  Create a JSON payload based on the `IngestStatusUpdate` model.
    For example, at [commit 3744ab3](https://github.com/wellcomecollection/storage-service/blob/3744ab37e8f11c8267448f5f76f48a548a2fb021/common/src/main/scala/weco/storage_service/ingests/models/IngestUpdate.scala), you'd create something like:

    ```json
    {
      "id": "710785f8-ee3d-4258-94b2-3b28eab73150",
      "status": {"type": "Failed"},
      "events": [
        {
          "description": "Manually marked as failed by $name for $reason",
          "createdDate": "$timestamp in the form 2001-01-01T01:01:01Z"
        }
      ],
      "type": "IngestStatusUpdate"
    }
    ```

3.  Send your JSON payload to the SNS topic which is the input for the `ingests_tracker` app.

    > **Note:** at Wellcome, these topics are `storage-prod_ingests` and `storage-staging_ingests`.
