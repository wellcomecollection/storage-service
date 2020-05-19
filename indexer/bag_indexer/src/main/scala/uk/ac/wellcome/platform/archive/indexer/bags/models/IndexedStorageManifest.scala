package uk.ac.wellcome.platform.archive.indexer.bags.models

import java.time.Instant

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest

case class IndexedStorageManifest(
  id: String,
  createdDate: Instant,
  @JsonKey("type") ontologyType: String = "Bag"
)

object IndexedStorageManifest {
  def apply(storageManifest: StorageManifest): IndexedStorageManifest =
    IndexedStorageManifest(
      id = storageManifest.id.toString,
      createdDate = storageManifest.createdDate
    )
}
