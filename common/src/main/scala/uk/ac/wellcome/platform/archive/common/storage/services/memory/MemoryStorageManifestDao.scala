package uk.ac.wellcome.platform.archive.common.storage.services.memory

import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.storage.services.{BetterStorageManifestDao, EmptyMetadata}
import uk.ac.wellcome.storage.store.HybridStoreEntry
import uk.ac.wellcome.storage.store.memory.MemoryVersionedStore

class MemoryStorageManifestDao(
  val vhs: MemoryVersionedStore[BagId, HybridStoreEntry[StorageManifest, EmptyMetadata]]
) extends BetterStorageManifestDao
