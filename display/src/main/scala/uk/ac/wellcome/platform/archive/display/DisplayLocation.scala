package uk.ac.wellcome.platform.archive.display

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.ingests.models
import uk.ac.wellcome.platform.archive.common.ingests.models.StorageLocation
import uk.ac.wellcome.storage.ObjectLocation

case class DisplayLocation(provider: DisplayProvider,
                           bucket: String,
                           path: String,
                           @JsonKey("type") ontologyType: String = "Location") {
  def toStorageLocation: StorageLocation =
    models.StorageLocation(
      provider.toStorageProvider,
      ObjectLocation(bucket, path))
}
object DisplayLocation {
  def apply(location: StorageLocation): DisplayLocation =
    DisplayLocation(
      provider = DisplayProvider(location.provider),
      bucket = location.location.namespace,
      path = location.location.path
    )
}
