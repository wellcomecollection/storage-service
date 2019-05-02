package uk.ac.wellcome.platform.archive.display

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.ingests.models.{Callback, Ingest}

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
