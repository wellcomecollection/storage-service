package uk.ac.wellcome.platform.archive.common.storage.services

import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.storage.store.{HybridStoreEntry, VersionedStore}
import uk.ac.wellcome.storage.{ReadError, WriteError}

// TODO: Do we need this wrapper at all now?
// TODO: This could be a Store!
class StorageManifestDao(
  vhs: VersionedStore[BagId,
                      Int,
                      HybridStoreEntry[StorageManifest, Map[String, String]]]
) {
  def get(id: BagId): Either[ReadError, StorageManifest] =
    vhs.getLatest(id).map { _.identifiedT.t }

  def put(storageManifest: StorageManifest): Either[WriteError, StorageManifest] =
    vhs
      .init(id = storageManifest.id)(HybridStoreEntry(storageManifest, metadata = Map.empty))
      .map { _.identifiedT.t }
}
