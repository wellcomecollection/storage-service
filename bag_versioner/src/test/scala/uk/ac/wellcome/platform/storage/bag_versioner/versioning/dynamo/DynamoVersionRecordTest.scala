package uk.ac.wellcome.platform.storage.bag_versioner.versioning.dynamo

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagId,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.storage.bag_versioner.versioning.VersionRecordGenerators

class DynamoVersionRecordTest
    extends AnyFunSpec
    with Matchers
    with VersionRecordGenerators {
  it("converts from a VersionRecord to a DynamoEntry and back") {
    val versionRecord = createVersionRecord

    DynamoVersionRecord(versionRecord).toVersionRecord shouldBe versionRecord
  }

  it("handles a VersionRecord with a colon in the externalIdentifier") {
    val versionRecord = createVersionRecordWith(
      externalIdentifier = ExternalIdentifier("x:y")
    )

    DynamoVersionRecord(versionRecord).toVersionRecord shouldBe versionRecord
  }

  it("handles a VersionRecord with a colon in the storageSpace") {
    val versionRecord = createVersionRecordWith(
      storageSpace = StorageSpace("x:y")
    )

    DynamoVersionRecord(versionRecord).toVersionRecord shouldBe versionRecord
  }

  it("creates human-readable IDs") {
    val versionRecord = createVersionRecordWith(
      externalIdentifier = ExternalIdentifier("x:y"),
      storageSpace = StorageSpace("a:b")
    )

    DynamoVersionRecord(versionRecord).id shouldBe BagId("a:b/x:y")
  }
}
