package uk.ac.wellcome.platform.archive.common

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation

sealed trait PipelinePayload {
  val ingestId: IngestID
}

case class IngestRequestPayload(
  ingestId: IngestID,
  ingestDate: Instant,
  storageSpace: StorageSpace,
  sourceLocation: ObjectLocation
) extends PipelinePayload

case object IngestRequestPayload {
  def apply(ingest: Ingest): IngestRequestPayload =
    IngestRequestPayload(
      ingestId = ingest.id,
      ingestDate = ingest.createdDate,
      storageSpace = StorageSpace(ingest.space.underlying),
      sourceLocation = ingest.sourceLocation.location
    )
}

case class UnpackedBagPayload(
  ingestId: IngestID,
  ingestDate: Instant,
  storageSpace: StorageSpace,
  unpackedBagLocation: ObjectLocation
) extends PipelinePayload

case object UnpackedBagPayload {
  def apply(ingestRequestPayload: IngestRequestPayload,
            unpackedBagLocation: ObjectLocation): UnpackedBagPayload =
    UnpackedBagPayload(
      ingestId = ingestRequestPayload.ingestId,
      ingestDate = ingestRequestPayload.ingestDate,
      storageSpace = ingestRequestPayload.storageSpace,
      unpackedBagLocation = unpackedBagLocation
    )
}

trait BagRootPayload extends PipelinePayload {
  val bagRootLocation: ObjectLocation
}

case class BagInformationPayload(
  ingestId: IngestID,
  storageSpace: StorageSpace,
  bagRootLocation: ObjectLocation,
  externalIdentifier: ExternalIdentifier,
  version: Int
) extends BagRootPayload
