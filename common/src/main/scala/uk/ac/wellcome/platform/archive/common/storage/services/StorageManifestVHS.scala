package uk.ac.wellcome.platform.archive.common.storage.services

import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.storage.ObjectStore
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.storage.vhs.{EmptyMetadata, VersionedHybridStore}

import scala.concurrent.{ExecutionContext, Future}

class StorageManifestVHS(
  underlying: VersionedHybridStore[StorageManifest,
                                   EmptyMetadata,
                                   ObjectStore[StorageManifest]]
)(implicit ec: ExecutionContext) {

  def getRecord(id: BagId): Future[Option[StorageManifest]] =
    underlying.getRecord(id = id.toString)

  def updateRecord(ifNotExisting: StorageManifest)(
    ifExisting: StorageManifest => StorageManifest): Future[Unit] =
    underlying
      .updateRecord(
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
      .map { _ =>
        ()
      }

  def insertRecord(storageManifest: StorageManifest): Future[Unit] =
    updateRecord(storageManifest)(_ => storageManifest)
}
