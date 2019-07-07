package uk.ac.wellcome.platform.archive.common.versioning.dynamo

import java.time.Instant

import org.scalatest.{FunSpec, Matchers, TryValues}
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.archive.common.versioning.VersionRecordGenerators

class DynamoVersionRecordTest
    extends FunSpec
    with Matchers
    with VersionRecordGenerators
    with TryValues {
  it("converts from a VersionRecord to a DynamoEntry and back") {
    val versionRecord = createVersionRecord

    DynamoVersionRecord(versionRecord).toVersionRecord.success.value shouldBe versionRecord
  }

  it("handles a VersionRecord with a colon in the externalIdentifier") {
    val versionRecord = createVersionRecordWith(
      externalIdentifier = ExternalIdentifier("x:y")
    )

    DynamoVersionRecord(versionRecord).toVersionRecord.success.value shouldBe versionRecord
  }

  it("handles a VersionRecord with a colon in the storageSpace") {
    val versionRecord = createVersionRecordWith(
      storageSpace = StorageSpace("x:y")
    )

    DynamoVersionRecord(versionRecord).toVersionRecord.success.value shouldBe versionRecord
  }

  it("creates human-readable IDs") {
    val versionRecord = createVersionRecordWith(
      externalIdentifier = ExternalIdentifier("x:y"),
      storageSpace = StorageSpace("a:b")
    )

    DynamoVersionRecord(versionRecord).id shouldBe "a%3Ab:x%3Ay"
  }

  it("throws an exception if the ID is malformed") {
    val entry = DynamoVersionRecord(
      id = "1234",
      ingestId = createIngestID,
      ingestDate = Instant.now(),
      version = 1
    )

    entry.toVersionRecord.failure.exception shouldBe a[
      IllegalArgumentException]
  }
}
