package weco.storage_service.display.ingests

import io.circe.generic.extras.JsonKey
import weco.storage_service.ingests.models.{Callback, Ingest}

case class DisplayStatus(
  id: String,
  @JsonKey("type") ontologyType: String = "Status"
)

object DisplayStatus {
  def apply(ingestStatus: Ingest.Status): DisplayStatus =
    DisplayStatus(ingestStatus.toString)

  def apply(callbackStatus: Callback.CallbackStatus): DisplayStatus =
    DisplayStatus(callbackStatus.toString)
}
