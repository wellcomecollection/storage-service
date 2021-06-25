package uk.ac.wellcome.platform.archive.display.ingests

import io.circe.generic.extras.JsonKey
import weco.storage_service.ingests.models.IngestEvent

case class DisplayIngestEvent(
  description: String,
  createdDate: String,
  @JsonKey("type") ontologyType: String = "IngestEvent"
)

case object DisplayIngestEvent {
  def apply(ingestEvent: IngestEvent): DisplayIngestEvent =
    DisplayIngestEvent(
      description = ingestEvent.description,
      createdDate = ingestEvent.createdDate.toString
    )
}
