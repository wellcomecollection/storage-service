package weco.storage_service.display.ingests

import io.circe.generic.extras.JsonKey
import weco.storage_service.ingests.models.Callback

case class DisplayCallback(
  url: String,
  status: Option[DisplayStatus],
  @JsonKey("type") ontologyType: String = "Callback"
)

object DisplayCallback {
  def apply(callback: Callback): DisplayCallback = DisplayCallback(
    url = callback.uri.toString,
    status = Some(DisplayStatus(callback.status))
  )
}
