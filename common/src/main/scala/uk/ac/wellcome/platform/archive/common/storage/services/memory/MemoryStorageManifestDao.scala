package uk.ac.wellcome.platform.archive.common.storage.services.memory

import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.storage.services.{EmptyMetadata, StorageManifestDao}
import uk.ac.wellcome.storage.{ReadError, Version}
import uk.ac.wellcome.storage.store.HybridStoreEntry
import uk.ac.wellcome.storage.store.memory.{MemoryStore, MemoryVersionedStore}

class MemoryStorageManifestDao(
  val vhs: MemoryVersionedStore[
    BagId,
    HybridStoreEntry[StorageManifest, EmptyMetadata]]
) extends StorageManifestDao {
  override def listVersions(bagId: BagId, before: Option[Int]): Either[ReadError, Seq[StorageManifest]] = {
    val underlying =
      vhs.store
        .asInstanceOf[MemoryStore[Version[BagId, Int], HybridStoreEntry[StorageManifest, EmptyMetadata]]]

    Right(
      underlying.entries
        .filter { case (_, HybridStoreEntry(manifest, _)) => manifest.id == bagId }
        .map { case (_, HybridStoreEntry(manifest, _)) => manifest }
        .toSeq
        .sortBy { _.version }
        .reverse
    )
  }
}
