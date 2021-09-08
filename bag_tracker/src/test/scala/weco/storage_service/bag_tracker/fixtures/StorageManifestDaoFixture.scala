package weco.storage_service.bag_tracker.fixtures

import weco.storage.store.memory._
import weco.storage_service.bag_tracker.storage.StorageManifestDao
import weco.storage_service.bag_tracker.storage.memory.MemoryStorageManifestDao
import weco.storage_service.bagit.models.BagId
import weco.storage_service.storage.models.StorageManifest

trait StorageManifestDaoFixture {
  def createStorageManifestDao(): StorageManifestDao =
    new MemoryStorageManifestDao(
      MemoryVersionedStore[BagId, StorageManifest](
        initialEntries = Map.empty
      )
    )
}
