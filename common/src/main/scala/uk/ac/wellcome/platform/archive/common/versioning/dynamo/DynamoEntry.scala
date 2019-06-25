package uk.ac.wellcome.platform.archive.common.versioning.dynamo

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.versioning.VersionRecord

import scala.util.Try

case class DynamoEntry(
  id: String,
  ingestId: IngestID,
  ingestDate: Instant,
  version: Int
) {
  def toVersionRecord: Try[VersionRecord] =
    for {
      storageSpace <- DynamoID.getStorageSpace(id)
      externalIdentifier <- DynamoID.getExternalIdentifier(id)
      record = VersionRecord(
        externalIdentifier = externalIdentifier,
        ingestId = ingestId,
        ingestDate = ingestDate,
        storageSpace = storageSpace,
        version = version
      )
    } yield record
}

case object DynamoEntry {
  def apply(versionRecord: VersionRecord): DynamoEntry =
    DynamoEntry(
      id = DynamoID.createId(
        storageSpace = versionRecord.storageSpace,
        externalIdentifier = versionRecord.externalIdentifier
      ),
      ingestId = versionRecord.ingestId,
      ingestDate = versionRecord.ingestDate,
      version = versionRecord.version
    )
}
