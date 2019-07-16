package uk.ac.wellcome.platform.archive.common.storage.services.memory

import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.storage.services.{
  EmptyMetadata,
  StorageManifestDao
}
import uk.ac.wellcome.storage.ReadError
import uk.ac.wellcome.storage.store.HybridStoreEntry
import uk.ac.wellcome.storage.store.memory.MemoryVersionedStore

class MemoryStorageManifestDao(
  val vhs: MemoryVersionedStore[
    BagId,
    HybridStoreEntry[StorageManifest, EmptyMetadata]]
) extends StorageManifestDao {
  override def listVersions(bagId: BagId, before: Option[Int]): Either[ReadError, Seq[StorageManifest]] = ???
}
