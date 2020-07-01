package uk.ac.wellcome.platform.archive.common.ingests.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagVersion,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace

case class Ingest(
  id: IngestID,
  ingestType: IngestType,
  sourceLocation: NewSourceLocation,
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
  sealed trait Completed extends Status

  case object Accepted extends Status {
    override def toString: String = "accepted"
  }

  case object Processing extends Status {
    override def toString: String = "processing"
  }

  case object Failed extends Completed {
    override def toString: String = "failed"
  }

  case object Succeeded extends Completed {
    override def toString: String = "succeeded"
  }
}
