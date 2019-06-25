package uk.ac.wellcome.platform.archive.common.versioning.dynamo

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.archive.common.versioning.VersionRecord

import scala.util.Try

case class DynamoEntry(
  id: String,
  ingestId: IngestID,
  ingestDate: Instant,
  version: Int
) {
  def toVersionRecord: Try[VersionRecord] = Try {
    id.split(":") match {
      case Array(storageSpace, externalIdentifier) =>
        VersionRecord(
          externalIdentifier = ExternalIdentifier(externalIdentifier),
          ingestId = ingestId,
          ingestDate = ingestDate,
          storageSpace = StorageSpace(storageSpace),
          version = version
        )

      case _ =>
        throw new IllegalArgumentException(
          s"Malformed ID for version record: $id"
        )
    }
  }
}

case object DynamoEntry {
  def apply(versionRecord: VersionRecord): DynamoEntry =
    DynamoEntry(
      id = s"${versionRecord.storageSpace}:${versionRecord.externalIdentifier}",
      ingestId = versionRecord.ingestId,
      ingestDate = versionRecord.ingestDate,
      version = versionRecord.version
    )
}
