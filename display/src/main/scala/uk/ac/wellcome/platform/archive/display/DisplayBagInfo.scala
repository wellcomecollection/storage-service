package uk.ac.wellcome.platform.archive.display

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier

case class RequestDisplayBagInfo(
  externalIdentifier: ExternalIdentifier,
  @JsonKey("type") ontologyType: String = "BagInfo"
)


case class ResponseDisplayBagInfo(
  externalIdentifier: ExternalIdentifier,
  @JsonKey("type") ontologyType: String = "BagInfo"
)
