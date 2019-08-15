package uk.ac.wellcome.platform.storage.bags.api.models

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.ingests.models.StorageLocation
import uk.ac.wellcome.platform.archive.display.DisplayProvider

case class DisplayStorageLocation(
  provider: DisplayProvider,
  bucket: String,
  path: String,
  @JsonKey("type") ontologyType: String = "Location"
)

case object DisplayStorageLocation {
  def apply(storageLocation: StorageLocation): DisplayStorageLocation =
    DisplayStorageLocation(
      provider = DisplayProvider(storageLocation.provider),
      bucket = storageLocation.location.namespace,
      path = storageLocation.location.path
    )
}
