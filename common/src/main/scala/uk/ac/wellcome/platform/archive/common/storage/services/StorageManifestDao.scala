package uk.ac.wellcome.platform.archive.common.storage.services

import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.storage.ReadError
import uk.ac.wellcome.storage.store.{HybridStoreEntry, VersionedStore}

class StorageManifestDao(
  vhs: VersionedStore[String,
                      Int,
                      HybridStoreEntry[StorageManifest, Map[String, String]]]
) {
  def get(id: String): Either[ReadError, StorageManifest] =
    vhs.getLatest(id.toString).map { _.identifiedT.t }

  def put(storageManifest: StorageManifest): vhs.WriteEither =
    vhs.init(id = storageManifest.id.toString)(
      HybridStoreEntry(storageManifest, metadata = Map.empty)
    )
}
