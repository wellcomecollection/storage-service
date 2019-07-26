package uk.ac.wellcome.platform.archive.common.versioning.dynamo

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.versioning.VersionRecord
import uk.ac.wellcome.storage.dynamo.DynamoHashRangeKeyPair

import scala.util.Try

case class DynamoVersionRecord(
  id: String,
  ingestId: IngestID,
  ingestDate: Instant,
  version: Int
) extends DynamoHashRangeKeyPair[String, Int] {
  def toVersionRecord: Try[VersionRecord] =
    for {
      storageSpace <- DynamoID.getStorageSpace(id)
      externalIdentifier <- DynamoID.getExternalIdentifier(id)
      record = VersionRecord(
        externalIdentifier = externalIdentifier,
        ingestId = ingestId,
        ingestDate = ingestDate,
        storageSpace = storageSpace,
        version = BagVersion(version)
      )
    } yield record

  // TODO: This type restriction is a bit and should be removed
  override val hashKey: String = id
  override val rangeKey: Int = version
}

case object DynamoVersionRecord {
  def apply(versionRecord: VersionRecord): DynamoVersionRecord =
    DynamoVersionRecord(
      id = DynamoID.createId(
        storageSpace = versionRecord.storageSpace,
        externalIdentifier = versionRecord.externalIdentifier
      ),
      ingestId = versionRecord.ingestId,
      ingestDate = versionRecord.ingestDate,
      version = versionRecord.version.underlying
    )
}
