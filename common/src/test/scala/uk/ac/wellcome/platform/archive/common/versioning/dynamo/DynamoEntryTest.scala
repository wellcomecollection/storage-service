package uk.ac.wellcome.platform.archive.common.versioning.dynamo

import java.time.Instant

import org.scalatest.{FunSpec, Matchers, TryValues}
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.archive.common.versioning.VersionRecordGenerators

class DynamoEntryTest extends FunSpec with Matchers with VersionRecordGenerators with TryValues {
  it("converts from a VersionRecord to a DynamoEntry and back") {
    val versionRecord = createVersionRecord

    DynamoEntry(versionRecord).toVersionRecord.success.value shouldBe versionRecord
  }

  it("handles a VersionRecord with a colon in the externalIdentifier") {
    val versionRecord = createVersionRecordWith(
      externalIdentifier = ExternalIdentifier("x:y")
    )

    DynamoEntry(versionRecord).toVersionRecord.success.value shouldBe versionRecord
  }

  it("handles a VersionRecord with a colon in the storageSpace") {
    val versionRecord = createVersionRecordWith(
      storageSpace = StorageSpace("x:y")
    )

    DynamoEntry(versionRecord).toVersionRecord.success.value shouldBe versionRecord
  }

  it("throws an exception if the ID is malformed") {
    val entry = DynamoEntry(
      id = "1234",
      ingestId = createIngestID,
      ingestDate = Instant.now(),
      version = 1
    )

    entry.toVersionRecord.failure.exception shouldBe a[IllegalArgumentException]
  }
}
