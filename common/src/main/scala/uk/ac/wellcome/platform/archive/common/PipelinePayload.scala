package uk.ac.wellcome.platform.archive.common

import java.net.URI

import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
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

case class CallbackNotification(
  ingestId: IngestID,
  callbackUri: URI,
  ingest: Ingest
) extends PipelinePayload
