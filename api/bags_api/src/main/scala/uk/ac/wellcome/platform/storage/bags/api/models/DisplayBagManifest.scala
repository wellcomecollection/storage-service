package uk.ac.wellcome.platform.storage.bags.api.models

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.bagit.models.BagManifest

case class DisplayBagManifest(
  checksumAlgorithm: String,
  files: Seq[DisplayFileDigest],
  @JsonKey("type")
  ontologyType: String = "BagManifest"
)

object DisplayBagManifest {
  def apply(bagManifest: BagManifest): DisplayBagManifest =
    DisplayBagManifest(
      checksumAlgorithm = bagManifest.checksumAlgorithm.value,
      files = bagManifest.files.map { DisplayFileDigest.apply }
    )
}
