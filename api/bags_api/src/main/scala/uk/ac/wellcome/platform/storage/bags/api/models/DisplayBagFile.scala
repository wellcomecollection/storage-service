package uk.ac.wellcome.platform.storage.bags.api.models

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.bagit.models.BagFile

case class DisplayBagFile(
  checksum: String,
  path: String,
  @JsonKey("type") ontologyType: String = "File"
)

case object DisplayBagFile {
  def apply(bagFile: BagFile): DisplayBagFile =
    DisplayBagFile(
      checksum = bagFile.checksum.value.value,
      path = bagFile.path.toString
    )
}
