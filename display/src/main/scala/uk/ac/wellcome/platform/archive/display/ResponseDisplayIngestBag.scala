package uk.ac.wellcome.platform.archive.display

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId

case class ResponseDisplayIngestBag(
  id: String,
  @JsonKey("type") ontologyType: String = "Bag"
)

object ResponseDisplayIngestBag {
  def apply(bagId: BagId): ResponseDisplayIngestBag =
    ResponseDisplayIngestBag(bagId.toString)
}
