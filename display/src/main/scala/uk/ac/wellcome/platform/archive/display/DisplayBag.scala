package uk.ac.wellcome.platform.archive.display

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier

case class DisplayBag(
  info: DisplayBagInfo,
  @JsonKey("type") ontologyType: String
)

case class DisplayBagInfo(
  externalIdentifier: ExternalIdentifier,
  @JsonKey("type") ontologyType: String
)
