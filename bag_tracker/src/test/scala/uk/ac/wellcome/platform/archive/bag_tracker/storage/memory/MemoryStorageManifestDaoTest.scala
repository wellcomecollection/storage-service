package uk.ac.wellcome.platform.archive.bag_tracker.storage.memory

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bag_tracker.storage.{
  StorageManifestDao,
  StorageManifestDaoTestCases
}
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.storage.store.memory.MemoryVersionedStore

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
