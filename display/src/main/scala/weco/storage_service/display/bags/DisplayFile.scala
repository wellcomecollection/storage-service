package weco.storage_service.display.bags

import io.circe.generic.extras.JsonKey
import weco.storage_service.storage.models.StorageManifestFile

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
