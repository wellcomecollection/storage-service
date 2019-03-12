package uk.ac.wellcome.platform.archive.display

import java.net.{URI, URL}
import java.util.UUID

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.ingests.models
import uk.ac.wellcome.platform.archive.common.ingests.models._
import uk.ac.wellcome.platform.archive.common.ingests.models._

sealed trait DisplayIngest

case class RequestDisplayIngest(sourceLocation: DisplayLocation,
                                callback: Option[DisplayCallback],
                                ingestType: DisplayIngestType,
                                space: DisplayStorageSpace,
                                @JsonKey("type")
                                ontologyType: String = "Ingest")
    extends DisplayIngest {
  def toIngest: Ingest = {
    models.Ingest(
      id = UUID.randomUUID,
      sourceLocation = sourceLocation.toStorageLocation,
      callback = Callback(
        callback.map(displayCallback => URI.create(displayCallback.url))),
      space = Namespace(space.id),
      status = Ingest.Accepted
    )
  }
}

case class ResponseDisplayIngest(@JsonKey("@context")
                                 context: String,
                                 id: UUID,
                                 sourceLocation: DisplayLocation,
                                 callback: Option[DisplayCallback],
                                 ingestType: DisplayIngestType,
                                 space: DisplayStorageSpace,
                                 status: DisplayStatus,
                                 bag: Option[IngestDisplayBag] = None,
                                 events: Seq[DisplayIngestEvent] = Seq.empty,
                                 createdDate: String,
                                 lastModifiedDate: String,
                                 @JsonKey("type")
                                 ontologyType: String = "Ingest")
    extends DisplayIngest

case class DisplayIngestMinimal(id: UUID,
                                createdDate: String,
                                @JsonKey("type")
                                ontologyType: String = "Ingest")

case class IngestDisplayBag(id: String,
                            @JsonKey("type")
                            ontologyType: String = "Bag")

case class DisplayCallback(url: String,
                           status: Option[DisplayStatus],
                           @JsonKey("type")
                           ontologyType: String = "Callback")

case class DisplayStorageSpace(id: String,
                               @JsonKey("type")
                               ontologyType: String = "Space")

case class DisplayStatus(id: String,
                         @JsonKey("type")
                         ontologyType: String = "Status")

case class DisplayIngestEvent(description: String,
                              createdDate: String,
                              @JsonKey("type")
                              ontologyType: String = "IngestEvent")

object ResponseDisplayIngest {
  def apply(ingest: Ingest, contextUrl: URL): ResponseDisplayIngest =
    ResponseDisplayIngest(
      context = contextUrl.toString,
      id = ingest.id,
      sourceLocation = DisplayLocation(ingest.sourceLocation),
      callback = ingest.callback.map(DisplayCallback(_)),
      space = DisplayStorageSpace(ingest.space.toString),
      ingestType = CreateDisplayIngestType,
      bag = ingest.bag.map(IngestDisplayBag(_)),
      status = DisplayStatus(ingest.status),
      events = ingest.events.map(DisplayIngestEvent(_)),
      createdDate = ingest.createdDate.toString,
      lastModifiedDate = ingest.lastModifiedDate.toString
    )
}

object DisplayIngestEvent {
  def apply(ingestEvent: IngestEvent): DisplayIngestEvent =
    DisplayIngestEvent(
      ingestEvent.description,
      ingestEvent.createdDate.toString)
}

object DisplayIngestMinimal {
  def apply(bagIngest: BagIngest): DisplayIngestMinimal =
    DisplayIngestMinimal(bagIngest.id, bagIngest.createdDate.toString)
}

object DisplayStatus {
  def apply(ingestStatus: Ingest.Status): DisplayStatus =
    DisplayStatus(ingestStatus.toString)

  def apply(callbackStatus: Callback.CallbackStatus): DisplayStatus =
    DisplayStatus(callbackStatus.toString)
}

object DisplayCallback {
  def apply(callback: Callback): DisplayCallback = DisplayCallback(
    callback.uri.toString,
    Some(DisplayStatus(callback.status))
  )
}

object IngestDisplayBag {
  def apply(bagId: BagId): IngestDisplayBag = IngestDisplayBag(bagId.toString)
}
