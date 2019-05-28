package uk.ac.wellcome.platform.archive.common.storage.services

import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.storage.vhs.{EmptyMetadata, VersionedHybridStore}
import uk.ac.wellcome.storage.{ReadError, StorageError}

class StorageManifestVHS(
  underlying: VersionedHybridStore[String, StorageManifest, EmptyMetadata]
) {

  def getRecord(id: BagId): Either[ReadError, StorageManifest] =
    underlying.get(id = id.toString)

  def updateRecord(ifNotExisting: StorageManifest)(
    ifExisting: StorageManifest => StorageManifest): Either[StorageError, Unit] =
    underlying
      .update(
        id = ifNotExisting.id.toString
      )(
        ifNotExisting = (ifNotExisting, EmptyMetadata())
      )(
        ifExisting = (existingStorageManifest: StorageManifest,
                      existingMetadata: EmptyMetadata) =>
          (
            ifExisting(existingStorageManifest: StorageManifest),
            existingMetadata: EmptyMetadata)
      )
      .map { _ => () }

  def insertRecord(storageManifest: StorageManifest): Either[StorageError, Unit] =
    updateRecord(storageManifest)(_ => storageManifest)
}
