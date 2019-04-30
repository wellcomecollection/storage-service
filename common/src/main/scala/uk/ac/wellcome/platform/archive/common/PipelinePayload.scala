package uk.ac.wellcome.platform.archive.common

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation

sealed trait PipelinePayload {
  val ingestId: IngestID
}

case class ObjectLocationPayload(
  ingestId: IngestID,
  storageSpace: StorageSpace,
  objectLocation: ObjectLocation
) extends PipelinePayload

case class BagInformationPayload(
  ingestId: IngestID,
  storageSpace: StorageSpace,
  objectLocation: ObjectLocation,
  externalIdentifier: ExternalIdentifier,
  version: Int
) {
  def bagRootLocation: ObjectLocation = objectLocation
}
