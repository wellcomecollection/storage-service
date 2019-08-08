package uk.ac.wellcome.platform.archive.common.storage.services.memory

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.storage.services.{
  EmptyMetadata,
  StorageManifestDao,
  StorageManifestDaoTestCases
}
import uk.ac.wellcome.storage.store.HybridStoreEntry
import uk.ac.wellcome.storage.store.memory.MemoryVersionedStore

class MemoryStorageManifestDaoTest
    extends StorageManifestDaoTestCases[MemoryVersionedStore[
      BagId,
      HybridStoreEntry[StorageManifest, EmptyMetadata]
    ]] {
  type MemoryStore =
    MemoryVersionedStore[
      BagId,
      HybridStoreEntry[StorageManifest, EmptyMetadata]
    ]

  override def withContext[R](testWith: TestWith[MemoryStore, R]): R =
    testWith(
      MemoryVersionedStore[BagId, HybridStoreEntry[
        StorageManifest,
        EmptyMetadata
      ]](initialEntries = Map.empty)
    )

  override def withDao[R](
    testWith: TestWith[StorageManifestDao, R]
  )(implicit store: MemoryStore): R =
    testWith(
      new MemoryStorageManifestDao(store)
    )
}
