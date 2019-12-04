package uk.ac.wellcome.platform.storage.bag_versioner.versioning.dynamo

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, BagVersion}
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.storage.bag_versioner.versioning.VersionRecord
import uk.ac.wellcome.storage.dynamo.DynamoHashRangeKeyPair

case class DynamoVersionRecord(
  id: BagId,
  ingestId: IngestID,
  ingestDate: Instant,
  version: Int
) extends DynamoHashRangeKeyPair[BagId, Int] {
  def toVersionRecord: VersionRecord =
    VersionRecord(
      externalIdentifier = id.externalIdentifier,
      ingestId = ingestId,
      ingestDate = ingestDate,
      storageSpace = id.space,
      version = BagVersion(version)
    )

  // TODO: This type restriction is a bit and should be removed
  override val hashKey: BagId = id
  override val rangeKey: Int = version
}

case object DynamoVersionRecord {
  def apply(versionRecord: VersionRecord): DynamoVersionRecord =
    DynamoVersionRecord(
      id = BagId(
        space = versionRecord.storageSpace,
        externalIdentifier = versionRecord.externalIdentifier
      ),
      ingestId = versionRecord.ingestId,
      ingestDate = versionRecord.ingestDate,
      version = versionRecord.version.underlying
    )
}
