package uk.ac.wellcome.platform.archive.common.ingests.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.{BagVersion, ExternalIdentifier}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace

case class Ingest(
  id: IngestID,
  ingestType: IngestType,
  sourceLocation: StorageLocation,
  space: StorageSpace,
  callback: Option[Callback],
  status: Ingest.Status,
  externalIdentifier: ExternalIdentifier,
  version: Option[BagVersion] = None,
  createdDate: Instant,
  events: Seq[IngestEvent] = Seq.empty
) {
  def lastModifiedDate: Option[Instant] =
    if (events.isEmpty) {
      None
    } else {
      Some(
        events.map { _.createdDate }.max
      )
    }
}

case object Ingest {
  sealed trait Status

  private val acceptedString = "accepted"
  private val processingString = "processing"
  private val succeededString = "succeeded"
  private val failedString = "failed"

  case object Accepted extends Status {
    override def toString: String = acceptedString
  }

  case object Processing extends Status {
    override def toString: String = processingString
  }

  case object Completed extends Status {
    override def toString: String = succeededString
  }

  case object Failed extends Status {
    override def toString: String = failedString
  }
}
