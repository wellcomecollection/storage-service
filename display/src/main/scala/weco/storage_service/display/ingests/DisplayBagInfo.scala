package weco.storage_service.display.ingests

import io.circe.generic.extras.JsonKey
import weco.storage_service.bagit.models.ExternalIdentifier

case class RequestDisplayBagInfo(
  externalIdentifier: String,
  @JsonKey("type") ontologyType: String = "BagInfo"
)

case class ResponseDisplayBagInfo(
  externalIdentifier: ExternalIdentifier,
  version: Option[String],
  @JsonKey("type") ontologyType: String = "BagInfo"
)
