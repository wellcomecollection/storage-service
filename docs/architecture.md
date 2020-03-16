# Architecture of the storage service

**Last updated: 16 March 2020.**

This document explains how the storage service works, at an individual application level.
It is intended for developers who are actively working on the storage service.

It follows the lifecycle of a bag: from initial conception to successfully stored.

![](images/pipeline.png)

At a high level, the storage service is a pipeline.
A message arrives at the first app asking the service to store a bag.
Each app does a small bit of work, then sends a message to the next app, which does another bit of work, and so on until the bag is successfully stored.

In our case, the messaging layer is Amazon SNS/SQS, but the principles apply to any messaging system.

In brief:

*   The user creates and uploads a bag as a tar.gz
*   They trigger an ingest through the ingests API
*   The bag unpacker unpacks the tar.gz into a temporary bucket




## Creating and uploading the bag

![](images/architecture/architecture.001.png)

The user selects files they want to store together, and creates a bag using the [BagIt packaging format](https://tools.ietf.org/html/rfc8493).
They compress the bag as a tar.gz archive, and upload it to an S3 bucket (the "ingests bucket").

<details>
  <summary>Who is "the user"?</summary>

  The user could be a person, but at Wellcome it's more likely to be an automated workflow management tool like Goobi or Archivematica.
</details>



## Triggering a new ingest

![](images/architecture/architecture.002.png)

An *ingest* is a request to the storage service for it to store a bag.

To trigger an ingest, the user calls the ingests API, passing several parameters:

*   The space and external identifier (part of the bag identifier)
*   The location of the bag in the S3 ingests bucket

The ingests API returns an HTTP 201 Created response, and the *ingest ID*.
The user can use the ingest ID to track the process of an ingest through the storage service.

In the turn, the ingests API records the ingest in two places:

*   It writes the ingest to the *ingests table*, a DynamoDB table that records the state of every ingest
*   It notifies the pipeline by sending a message to an SNS topic (specifically, an SNS topic that is read by the bag unpacker)



## Interlude: tracking the state of ingests

The user can use the ingest ID to ask the ingests API for the state of an ingest.
But how do we track the state of an ingest through the pipeline?

![](images/architecture/architecture.003.png)

Each app in the pipeline can send messages to two SNS topics:

*   Messages for the ingests monitor (*"I've done this piece of work on the bag"* or *"Something went wrong and I couldn't process the bag"*)
*   Messages for the next app in the pipeline (*"I've finished working with this bag, it's your turn to do something"*).

The ingests monitor is another app which sits outside the pipeline.
It reads messages from the ingests topic/queue, and writes the updates into the ingests table.
When the user asks the ingests API for the state of an ingest, it reads the ingest from this table.




## Unpacking the compressed bag

The first app in the pipeline is the **bag unpacker**.
The user uploads a bag as a compressed tar.gz archive.
This app unpacks the contents of that archive into a temporary bucket, then sends the location of the unpacked bag to the next app.

<details>
  <summary>Why do we unpack into a temporary bucket, not permanent storage?</summary>

  At this point, we don't know what the tar.gz contains.
  It could be missing files, have incorrect checksums, or not be a bag at all!
  The permanent storage should only be for verified, known-good bags.
  We don't want to pollute it with bags that we'll reject later.

  Also, copying the bag into storage we control means the user can't modify it midway through the process.
  It would be an issue if, say, we verified a bag, and then the user uploaded a new version before we'd copied it into the permanent storage.
</details>

<details>
  <summary>Why do we require tar.gz as the compression format?</summary>

  Early versions of the storage service used zip as the compression format, but you can't unpack a zip archive unless you have random access into the archive.
  That means you either download the whole archive to disk (impractical) or implement random access into S3 objects using Ranged GET requests (fiddly).

  Using tar.gz allows you to stream the bytes out, and unpack as you go.
</details>

<details>
  <summary>Do bags need to be compressed at all?</summary>

  Compressing/uncompressing a bag does add some overhead.
  In theory, the storage service could take an uncompressed bag as input, but we'd still want to copy it into storage we control before running an initial verification, so the user doesn't modify it mid-ingest.
</details>



## Finding the root of the unpacked bag

The top level files in a bag are:

*   The metadata files (`bagit.txt`, `bag-info.txt`, the manifests and tag manifests)
*   The payload directory (`data/`)

Depending on how the user compressed the bag as a tar.gz, these entries might be at the top-level, or they might be in an enclosing folder.
The **bag root finder** tries to find the actual root of the bag, and sends that location to the next app.



## Verifying the bag

## Assigning a version to the bag

Why assign a version here?

## Replicating and verifying the bag into permanent storage

## Registering the bag

## Optional: making a callback to the user
