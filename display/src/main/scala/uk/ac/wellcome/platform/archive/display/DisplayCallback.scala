package uk.ac.wellcome.platform.archive.display
import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.ingests.models.Callback

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
