package uk.ac.wellcome.platform.archive.common.storage.services

import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.storage.{ReadError, StorageError}
import uk.ac.wellcome.storage.vhs.{EmptyMetadata, VersionedHybridStore}

class StorageManifestDao(
  vhs: VersionedHybridStore[String, StorageManifest, EmptyMetadata]
) {
  def get(id: BagId): Either[ReadError, StorageManifest] = vhs.get(s"$id")

  def put(storageManifest: StorageManifest): Either[StorageError, Unit] = {

    val ifNotExisting = (storageManifest, EmptyMetadata())
    val ifExisting = (o: StorageManifest, m: EmptyMetadata) => (o, m)

    vhs.update(s"${storageManifest.id}")(ifNotExisting)(ifExisting).map {
      _ => ()
    }

  }
}

