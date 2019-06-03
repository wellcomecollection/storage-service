package uk.ac.wellcome.platform.archive.display

import java.net.{URI, URL}
import java.util.UUID

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.ingests.models._

sealed trait DisplayIngest

case class RequestDisplayIngest(sourceLocation: DisplayLocation,
                                callback: Option[DisplayCallback],
                                ingestType: DisplayIngestType,
                                space: DisplayStorageSpace,
                                @JsonKey("type")
                                ontologyType: String = "Ingest")
    extends DisplayIngest {
  def toIngest: Ingest =
    Ingest(
      id = IngestID.random,
      sourceLocation = sourceLocation.toStorageLocation,
      callback = Callback(
        callback.map(displayCallback => URI.create(displayCallback.url))),
      space = Namespace(space.id),
      status = Ingest.Accepted
    )
}

case class ResponseDisplayIngest(@JsonKey("@context") context: String,
                                 id: UUID,
                                 sourceLocation: DisplayLocation,
                                 callback: Option[DisplayCallback],
                                 ingestType: DisplayIngestType,
                                 space: DisplayStorageSpace,
                                 status: DisplayStatus,
                                 bag: Option[ResponseDisplayIngestBag] = None,
                                 events: Seq[DisplayIngestEvent] = Seq.empty,
                                 createdDate: String,
                                 lastModifiedDate: String,
                                 @JsonKey("type") ontologyType: String =
                                   "Ingest")
    extends DisplayIngest

object ResponseDisplayIngest {
  def apply(ingest: Ingest, contextUrl: URL): ResponseDisplayIngest =
    ResponseDisplayIngest(
      context = contextUrl.toString,
      id = ingest.id.underlying,
      sourceLocation = DisplayLocation(ingest.sourceLocation),
      callback = ingest.callback.map { DisplayCallback(_) },
      space = DisplayStorageSpace(ingest.space.toString),
      ingestType = CreateDisplayIngestType,
      bag = ingest.bag.map { ResponseDisplayIngestBag(_) },
      status = DisplayStatus(ingest.status),
      events = ingest.events
        .sortBy { _.createdDate }
        .map { DisplayIngestEvent(_) },
      createdDate = ingest.createdDate.toString,
      lastModifiedDate = ingest.lastModifiedDate.toString
    )
}
