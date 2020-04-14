package uk.ac.wellcome.platform.storage.bags.api.models

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.storage.models.FileManifest

case class DisplayFileManifest(
  checksumAlgorithm: String,
  files: Seq[DisplayFile],
  @JsonKey("type") ontologyType: String = "BagManifest"
)

object DisplayFileManifest {
  def apply(fileManifest: FileManifest): DisplayFileManifest =
    DisplayFileManifest(
      checksumAlgorithm = fileManifest.checksumAlgorithm.value,
      files =
        fileManifest.files
          .sortBy { _.name }
          .map { DisplayFile.apply }
    )
}
