package uk.ac.wellcome.platform.archive.display

import java.net.{URI, URL}
import java.time.Instant
import java.util.UUID

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.ingests.models._
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace

sealed trait DisplayIngest

case class RequestDisplayIngest(
  sourceLocation: DisplayLocation,
  callback: Option[DisplayCallback],
  ingestType: DisplayIngestType,
  space: DisplayStorageSpace,
  externalIdentifier: String,
  @JsonKey("type")
  ontologyType: String = "Ingest"
) extends DisplayIngest {
  def toIngest: Ingest =
    Ingest(
      id = IngestID.random,
      ingestType = IngestType.create(ingestType.id),
      sourceLocation = sourceLocation.toStorageLocation,
      callback = Callback(
        callback.map(displayCallback => URI.create(displayCallback.url))),
      space = StorageSpace(space.id),
      externalIdentifier = ExternalIdentifier(externalIdentifier),
      status = Ingest.Accepted,
      createdDate = Instant.now
    )
}

case class ResponseDisplayIngest(@JsonKey("@context") context: String,
                                 id: UUID,
                                 sourceLocation: DisplayLocation,
                                 callback: Option[DisplayCallback],
                                 ingestType: DisplayIngestType,
                                 space: DisplayStorageSpace,
                                 status: DisplayStatus,
                                 externalIdentifier: String,
                                 events: Seq[DisplayIngestEvent] = Seq.empty,
                                 createdDate: String,
                                 lastModifiedDate: Option[String],
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
      ingestType = DisplayIngestType(ingest.ingestType),
      externalIdentifier = ingest.externalIdentifier.underlying,
      status = DisplayStatus(ingest.status),
      events = ingest.events
        .sortBy { _.createdDate }
        .map { DisplayIngestEvent(_) },
      createdDate = ingest.createdDate.toString,
      lastModifiedDate = ingest.lastModifiedDate.map { _.toString }
    )
}
