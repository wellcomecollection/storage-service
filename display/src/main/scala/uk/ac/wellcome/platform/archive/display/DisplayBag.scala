package uk.ac.wellcome.platform.archive.display

import io.circe.generic.extras.JsonKey

case class RequestDisplayBag(
  info: RequestDisplayBagInfo,
  @JsonKey("type") ontologyType: String = "Bag"
)


case class ResponseDisplayBag(
  info: ResponseDisplayBagInfo,
  @JsonKey("type") ontologyType: String = "Bag"
)
