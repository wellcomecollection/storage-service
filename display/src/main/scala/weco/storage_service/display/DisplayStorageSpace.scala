package weco.storage_service.display
import io.circe.generic.extras.JsonKey

case class DisplayStorageSpace(
  id: String,
  @JsonKey("type") ontologyType: String = "Space"
)
