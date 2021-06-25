package uk.ac.wellcome.platform.archive.bag_tracker.storage.memory

import weco.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bag_tracker.storage.{
  StorageManifestDao,
  StorageManifestDaoTestCases
}
import weco.storage_service.bagit.models.BagId
import weco.storage_service.storage.models.StorageManifest
import weco.storage.store.memory.MemoryVersionedStore

class MemoryStorageManifestDaoTest
    extends StorageManifestDaoTestCases[MemoryVersionedStore[
      BagId,
      StorageManifest
    ]] {
  type MemoryStore =
    MemoryVersionedStore[
      BagId,
      StorageManifest
    ]

  override def withContext[R](testWith: TestWith[MemoryStore, R]): R =
    testWith(
      MemoryVersionedStore[BagId, StorageManifest](initialEntries = Map.empty)
    )

  override def withDao[R](
    testWith: TestWith[StorageManifestDao, R]
  )(implicit store: MemoryStore): R =
    testWith(
      new MemoryStorageManifestDao(store)
    )
}
