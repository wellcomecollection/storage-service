package uk.ac.wellcome.platform.archive.display.ingests

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier

case class RequestDisplayBagInfo(
  externalIdentifier: String,
  @JsonKey("type") ontologyType: String = "BagInfo"
)

case class ResponseDisplayBagInfo(
  externalIdentifier: ExternalIdentifier,
  version: Option[String],
  @JsonKey("type") ontologyType: String = "BagInfo"
)
