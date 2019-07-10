package uk.ac.wellcome.platform.storage.bags.api.models

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifestFile

case class DisplayFile(
  checksum: String,
  name: String,
  path: String,
  @JsonKey("type") ontologyType: String = "File"
)

case object DisplayFile {
  def apply(file: StorageManifestFile): DisplayFile =
    DisplayFile(
      checksum = file.checksum.value,
      name = file.name,
      path = file.path
    )
}
