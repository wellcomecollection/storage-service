# Using multiple storage tiers for cost-efficiency (A/V, TIFFs)

Within [our warm replica](https://app.gitbook.com/o/-LumfFcEMKx4gYXKAZTQ/s/5fJiiTl4PgHkFAzFiHc8/~/changes/1/wellcome-specific-information/our-storage-configuration/replica-configuration), we can store content as a mixture of Standard-IA and Glacier; this is primarily for cost efficiency. Storing objects in Glacier is [approximately 3.5x cheaper](https://aws.amazon.com/s3/pricing/) than storing them in Standard-IA.

# Use case

At time of writing (May 2023), there are two use cases for this feature:

*   **Digitised A/V.** Our digitised A/V workflow produces both a high-resolution MXF and a lower-resolution MP4.

    -   The MP4 is the "access copy" – if somebody is watching the video through DLCS, it’s being transcoded from the MP4.
    -   The MXF is the "preservation copy" – it's considered the canonical copy of the video and we could use it to create new access copies in the future, but it's too big to serve in a sensible way (some of the files are >100GB). We don't need immediate access to it.

    We store the MP4 in Standard-IA and the MXF in Glacier.

*   **Digitised manuscripts.** In our digitised manuscripts workflow, we keep both the original TIFF and the edited JP2 from LayoutWizzard.

    -   The JP2 is the "access copy" used by DLCS to serve images on the web
    -   The TIFF is the "preservation copy" that we don't access on a day-to-day basis.

    We store the JP2s in Standard-IA and the TIFFs in Glacier.

You can see the current set in [the TagRules object](https://github.com/wellcomecollection/storage-service/blob/main/bag_tagger/src/main/scala/weco/storage_service/bag_tagger/services/TagRules.scala) in the bag tagger.

# How it works

*   When the bag register finishes storing a bag, it sends a notification *"We've successfully stored a new bag in space `X` with identifier `Y` and version `Z`"*
*   The bag tagger picks up this message, and applies key-value tags to certain objects in the newly stored bag, e.g. we add `Content-Type: application/mxf` for our high-resolution MXF video files.
*   We set up S3 lifecycle configuration rules on our storage buckets to transition objects with certain tags into the Glacier storage tier, e.g. *"Move any object with the tag `Content-Type: application/mxf` to Glacier 90 days after it was created."*
