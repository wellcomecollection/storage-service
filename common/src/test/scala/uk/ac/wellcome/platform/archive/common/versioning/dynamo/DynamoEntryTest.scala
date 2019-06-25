package uk.ac.wellcome.platform.archive.common.versioning.dynamo

import org.scalatest.{FunSpec, Matchers, TryValues}
import uk.ac.wellcome.platform.archive.common.versioning.VersionRecordGenerators

class DynamoEntryTest extends FunSpec with Matchers with VersionRecordGenerators with TryValues {
  it("converts from a VersionRecord to a DynamoEntry and back") {
    val versionRecord = createVersionRecord

    DynamoEntry(versionRecord).toVersionRecord.success.value shouldBe versionRecord
  }
}
