package uk.ac.wellcome.platform.archive.display.bags

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifestFile

case class DisplayFile(
  checksum: String,
  name: String,
  path: String,
  size: Long,
  @JsonKey("type") ontologyType: String = "File"
)

case object DisplayFile {
  def apply(file: StorageManifestFile): DisplayFile =
    DisplayFile(
      checksum = file.checksum.value,
      name = file.name,
      path = file.path,
      size = file.size
    )
}
