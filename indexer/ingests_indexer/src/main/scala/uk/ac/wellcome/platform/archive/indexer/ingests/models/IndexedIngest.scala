package uk.ac.wellcome.platform.archive.indexer.ingests.models
import java.util.UUID

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.display.{DisplayLocation, DisplayStorageSpace}
import uk.ac.wellcome.platform.archive.display.ingests._

// This should mirror the DisplayIngest class as much as possible, but
// we may expose extra fields that we don't expose in the API if they're
// useful for reporting.
case class IndexedIngest(
  id: UUID,
  sourceLocation: DisplayLocation,
  callback: Option[DisplayCallback],
  ingestType: DisplayIngestType,
  space: DisplayStorageSpace,
  status: DisplayStatus,
  bag: ResponseDisplayBag,
  events: Seq[DisplayIngestEvent] = Seq.empty,
  createdDate: String,
  lastModifiedDate: Option[String],
  @JsonKey("type") ontologyType: String = "Ingest",

  // We expose a summary of the failure descriptions so we can aggregate on them
  // in Elasticsearch, and see which apps are the source of errors.
  failureDescriptions: Option[String] = None
)

case object IndexedIngest {
  def apply(ingest: Ingest): IndexedIngest = {
    val failureDescriptions =
      ingest.events
        .map { _.description }
        .filter { _.contains("failed") }

    val indexedFailureDescriptions = failureDescriptions match {
      case Nil => None
      case _   => Some(failureDescriptions.mkString(", "))
    }

    IndexedIngest(
      id = ingest.id.underlying,
      sourceLocation = DisplayLocation(ingest.sourceLocation),
      callback = ingest.callback.map { DisplayCallback(_) },
      space = DisplayStorageSpace(ingest.space.toString),
      ingestType = DisplayIngestType(ingest.ingestType),
      bag = ResponseDisplayBag(
        info = ResponseDisplayBagInfo(
          externalIdentifier = ingest.externalIdentifier,
          version = ingest.version.map { _.toString }
        )
      ),
      status = DisplayStatus(ingest.status),
      events = ingest.events
        .sortBy { _.createdDate }
        .map { DisplayIngestEvent(_) },
      createdDate = ingest.createdDate.toString,
      lastModifiedDate = ingest.lastModifiedDate.map { _.toString },
      failureDescriptions = indexedFailureDescriptions
    )
  }
}
