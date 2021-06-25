package uk.ac.wellcome.platform.storage.bags.api.models

import io.circe.generic.extras.JsonKey
import weco.storage_service.bag_tracker.models.BagVersionEntry
import weco.storage_service.bagit.models.BagId
import weco.storage_service.storage.models.StorageManifest

case class DisplayBagVersionEntry(
  id: String,
  version: String,
  createdDate: String,
  @JsonKey("type") ontologyType: String = "Bag"
)

case object DisplayBagVersionEntry {
  def apply(manifest: StorageManifest): DisplayBagVersionEntry =
    DisplayBagVersionEntry(
      id = manifest.id.toString,
      version = manifest.version.toString,
      createdDate = manifest.createdDate.toString
    )

  def apply(id: BagId, entry: BagVersionEntry): DisplayBagVersionEntry =
    DisplayBagVersionEntry(
      id = id.toString,
      version = entry.version.toString,
      createdDate = entry.createdDate.toString
    )
}
