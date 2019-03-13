package uk.ac.wellcome.platform.archive.common.ingests.models

import java.time.Instant
import java.util.UUID

import uk.ac.wellcome.platform.archive.common.bagit.models.BagId

case class Ingest(id: UUID,
                  sourceLocation: StorageLocation,
                  space: Namespace,
                  callback: Option[Callback],
                  status: Ingest.Status,
                  bag: Option[BagId],
                  createdDate: Instant,
                  lastModifiedDate: Instant,
                  events: Seq[IngestEvent])

case object Ingest {
  def apply(id: UUID,
            sourceLocation: StorageLocation,
            space: Namespace,
            callback: Option[Callback] = None,
            status: Ingest.Status = Ingest.Accepted,
            bag: Option[BagId] = None,
            createdDate: Instant = Instant.now(),
            events: Seq[IngestEvent] = Seq.empty): Ingest = {
    Ingest(
      id,
      sourceLocation,
      space,
      callback,
      status,
      bag,
      createdDate,
      createdDate,
      events)
  }

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

case class BagIngest(bagIdIndex: String, id: UUID, createdDate: Instant)
