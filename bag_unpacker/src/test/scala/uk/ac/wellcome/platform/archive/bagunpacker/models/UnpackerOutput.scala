package uk.ac.wellcome.platform.archive.bagunpacker.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.IngestRequestPayload
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation

case class UnpackerOutput(
  ingestId: IngestID,
  ingestDate: Instant,
  storageSpace: StorageSpace,
  unpackedBagLocation: ObjectLocation
)

case object UnpackerOutput {
  def apply(ingestRequestPayload: IngestRequestPayload,
            unpackedBagLocation: ObjectLocation): UnpackerOutput =
    UnpackerOutput(
      ingestId = ingestRequestPayload.ingestId,
      ingestDate = ingestRequestPayload.ingestDate,
      storageSpace = ingestRequestPayload.storageSpace,
      unpackedBagLocation = unpackedBagLocation
    )
}
