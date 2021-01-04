import datetime


def get_dev_status(ingest):
    """
    Get an ingest status that reflects whether we need to pay attention to it.
    For example:

    *   If the ingest has failed, was it a user error (failed verification) or
        a storage service error (failed replication)?
    *   If the ingest is processing or accepted, has it been updated recently,
        or is it stalled?

    """
    # Success never needs our attention.
    if ingest["status"] == "succeeded":
        return "succeeded"

    elif ingest["status"] == "failed":
        # We sort failures into two groups:
        #
        #   -   a known/user error is one that means there was something wrong with the
        #       bag, e.g. it couldn't be unpacked correctly, it failed verification
        #   -   an unknown error is one that we can't categorise, and might indicate
        #       a storage service error, e.g. a replication failure
        #
        failure_reasons = [
            ev["description"]
            for ev in ingest["events"]
            if "failed" in ev["description"]
        ]

        if failure_reasons and all(
            reason.startswith(
                (
                    "Verification (pre-replicating to archive storage) failed",
                    "Detecting bag root failed",
                    "Callback failed for:",
                    # If we can't unpack a bag or assign a version for an
                    # unknown reason, we should treat that as a storage service error.
                    "Unpacking failed -",
                    "Assigning bag version failed -",
                )
            )
            for reason in failure_reasons
        ):
            return "failed (user error)"

        # Handle the case where something went wrong, and we marked the bag as manually
        # failed.
        if failure_reasons and failure_reasons[-1].startswith(
            (
                "Ingest manually marked as failed",
                "Manually marked as failed",
            )
        ):
            return "failed (known error)"

        if is_known_failure(ingest["id"]):
            return "failed (known error)"

        # Handle the case where the unpacking was failing, and we had to patch the
        # unpacker to add a user-facing failure reason.
        #
        # e.g. "Unpacking failed", "Unpacking failed", "Unpacking failed - Unexpected EOF"
        #
        # If we got a user-facing message *eventually*, then we can treat this as
        # a user error that we handled.
        if (
            failure_reasons
            and all(reason.startswith("Unpacking failed") for reason in failure_reasons)
            and failure_reasons[-1].startswith("Unpacking failed -")
        ):
            return "failed (user error)"

        else:
            return "failed (unknown reason)"

    elif ingest["status"] == "accepted":
        # An ingest is in the 'accepted' state until it goes to the bag unpacker.
        # There may be a short delay while the bag unpacker starts up; a delay of
        # more than an hour suggests something is wrong.
        #
        # To allow for timezone slop, look for a delay of two hours.
        delay = datetime.datetime.now() - ingest["createdDate"]

        if abs(delay.total_seconds()) > 60 * 60 * 2:
            return "stalled"
        else:
            return "accepted"

    elif ingest["status"] == "processing":
        # Ingests should wait up to 5 hours before being retried due to SQS.
        # If an ingest hasn't been updated in more than 5 hours, something is
        # probably wrong.
        #
        # To allow for timezone slop, look for a delay of seven hours.  It will
        # be flagged the following day if it's still stalled.
        delay = datetime.datetime.now() - ingest["createdDate"]

        if abs(delay.total_seconds()) > 60 * 60 * 7:
            return "stalled"
        else:
            return "processing"


def is_known_failure(ingest_id):
    return ingest_id in {
        # digitised/b12948421, failed with an unknown unpacker error
        # Reingested as f9850ea4-28b3-4ef2-a385-d0227fb5167a
        "dda879ab-ba7b-4e87-a34b-6511e6e1457b",
        # born-digital-accessions/2535, failed with an unknown unpacker error
        # Reingested as d3e9474f-9680-4d83-b214-811afe07db25
        "bd5c4bfe-8838-49dc-9a5a-4c10a0770f9c",
        # digitised/b13147006, never started
        # Reingested as a4bb6084-3d5a-4a3e-8006-88baf0f67207
        "84ae9145-45d7-4f11-afa1-c25a0dd264df",
        # digitised/b16732157
        # The bag failed at the bag register because a fetch.txt'd file was in the
        # wrong prefix; this failure mode is now checked in the bag verifier.
        # Successfully ingested as ee12c44b-ec17-4ace-ae6f-1bb92a4fb162
        "7d0595b8-166a-4bb3-83cb-8200beb1c4a4",
        # digitised/b12812274, failed with an unknown unpacker error
        # Successfully ingested as 59cddef7-8d46-49a3-9fb7-b38a3769c04c
        "79ed6392-fbd0-452c-ae88-143425dff7a8",
        #
        # In old versions of the storage service, the bag versioner wouldn't show
        # a user-facing error message.  This has now been changed: the versioner
        # should explain why the bag versioner failed, and if a new bag fails with
        # an unexplained versioner error, we should flag it.
        #
        # These bags failed before we made this change, usually because another
        # bag with the same externalIdentifier was being ingested simultaneously.
        #
        # digitised/b18314934 ~ b8a1e20f-8f1b-425d-913e-65683bff2d55
        "5a40c51f-8729-409d-8ac7-889cc5366ebc",
        # testing/test_bag
        "44206fc6-5916-4399-b863-4ac3a4195b49",
        "c9f1cc27-7da3-43cf-8584-04c45a7db67d",
        # born-digital/PBLBIO/A/19 ~ 563ca3b2-e6c9-482b-8c37-830fb3ee3a56
        "43052ab1-4a59-4c0a-8140-af0f762e0234",
        # digitised/b18031870 ~ f8391482-a85b-401c-bc24-ac91bba410fa
        "3e4f8736-956e-435d-8225-c22f9d27d555",
        # born-digital/PPARD ~ db530ccd-d1f9-43e6-b3e7-c7af288bb7c2
        "cb795420-987a-438b-88c6-5ab967765dae",
        # Bags in the staging service
        "b7bde3b7-ab5b-46cd-a597-5beb5bde6899",
        "b4a79b39-925c-4b6a-ad29-b79ccab6a9e7",
        "ae57957d-2600-497d-9767-b71fafa2a0bb",
        "8611deb8-8202-4586-9ba1-3227e8d0ac59",
        "aae87150-1d60-456f-b9f3-65c0584fbeea",
        "5f4d0b77-2c9a-473e-b7c3-9a9175bc5d05",
        "59ff15cb-67b4-4fe2-af56-d107525f882d",
        "49fa0b00-7cd7-4be1-95f9-46d95d93e0b7",
        #
        # A bunch of S3 verifications failures in the staging service.
        # I suspect these are caused by S3 consistency issues which have since
        # been fixed (both in our code and in S3 itself); I don't plan to
        # investigate further.
        "ef50ccc3-68a8-4be8-8d27-cc3956fd877a",
        "eb0ec23b-26e1-4379-b4fe-aec107f461ab",
        "d4f840c0-73d8-4efc-9078-fb231300cea4",
        #
        # Other failures in the staging service that I don't plan to investigate right now.
        #
        # Aggregating replicas failed
        "e8c45e88-eb6a-4857-ad0c-963375e3e96e",
        "d4c9ae0b-5a8b-4553-8c2e-94c70321dd88",
        "ae57957d-2600-497d-9767-b71fafa2a0bb",
        "8488e013-9a27-4525-8e93-682eed63bf2e",
        "a406b57a-2aa8-44af-b9f2-533397e39be2",
        "5ca383ba-8adf-4dca-84c6-8c1278e42ad7",
        "4c582387-7c9d-4c40-a822-1222e08410d7",
        "02f54df9-1570-4a2c-81fe-09cca29cd5e2",
        #
        # Register failed
        "e50dc0aa-2a73-48dc-aa3d-f67fee34b59b",
        "dc9c6d7b-7018-4b32-a5be-6d80e1b9b0c4",
        "d44ba152-d96a-48c9-bdb2-8fd21f8152f0",
        "d3f79d7e-f868-4c5a-a6ff-571402c46b3a",
        "d2c8d63f-9e32-4997-9a22-6053452e428b",
        "d08fbd1c-124a-4e35-b53b-457f86770b75",
        "cf8e1810-665b-4664-95c3-f3e62d65063f",
        "c1c23437-81be-4ab1-9490-12cff77df67c",
        "be60915d-dd64-4f16-9d21-43632c57185a",
        "bd3ead10-31f3-443e-8680-f718e3d4fb74",
        "b88419e0-77c0-43fc-9d16-8f9181f3549a",
        "aedbf0d8-ddcc-4071-a511-23cf44e2c082",
        "ae57957d-2600-497d-9767-b71fafa2a0bb",
        "975dc902-fec6-4bbb-95c6-7362b2846ed3",
        "8bf329dd-ddc9-4a19-ab9c-d05c1cccd418",
        "828950c8-bd9d-4335-b40c-01669c1bc737",
        "82347cf6-cece-496a-bc8a-713ae5eb1624",
        "7556664f-b318-4a85-8995-8ab35d332b72",
        "73004e63-cb89-43b3-8517-1f4eb8a9edcb",
        "6b01e974-a724-41d6-9544-8ccedfe850a0",
        "63e8b814-c5fe-48d6-a443-752dad7eb51b",
        "5dbccbf2-b5f6-497f-9819-5747506fb0fc",
        "5bdd4065-9da3-43b5-a50e-3581fc7042f7",
        "5b788b9c-26f1-49f4-a399-f51ae0374d1e",
        "4dcf1fa6-9a97-4f81-8b64-d33c20c16a07",
        "47788181-be33-427b-beea-509dba7472ed",
        "4706b23b-3fbb-4056-97cd-b681f11662e9",
        "2b67083d-f1e6-4f71-b15b-b31d2ec92098",
        "2172e3e0-5ec9-476f-aa42-f6200c5762cd",
        "1fac4dc8-0881-4fa8-87f1-bf310ed778ac",
        "1e11c67e-455b-48e3-b613-77a9bae99330",
        "1d47eaf4-a067-44ee-b9ca-823bc63c663c",
        "1c1506b0-3b45-453b-855e-3a1b70940bbf",
        #
        # Azure verification/replication failed
        "c3277c2c-e2a3-4a18-a132-b726f0bc1741",
        "75d5ad51-ea2b-44ff-8f19-ab75d854218e",
        "7533328e-62a0-4bd9-86ae-329a6c962d13",
        "63780981-f120-4b40-8ded-c8f81dd75581",
        "36a9dd99-72e3-4463-97b4-aab08be45599",
    }
