package uk.ac.wellcome.platform.archive.common.storage

import uk.ac.wellcome.platform.archive.common.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.models.bagit.BagId
import uk.ac.wellcome.storage.ObjectStore
import uk.ac.wellcome.storage.vhs.{EmptyMetadata, VersionedHybridStore}

import scala.concurrent.{ExecutionContext, Future}

class StorageManifestVHS(
  vhs: VersionedHybridStore[StorageManifest,
                            EmptyMetadata,
                            ObjectStore[StorageManifest]]
)(implicit ec: ExecutionContext) {

  def getRecord(id: BagId): Future[Option[StorageManifest]] =
    vhs.getRecord(id = id.toString)

  def updateRecord(
    ifNotExisting: StorageManifest)(
    ifExisting: StorageManifest => StorageManifest): Future[Unit] =
    vhs.updateRecord(
      id = ifNotExisting.id.toString
    )(
      ifNotExisting = (ifNotExisting, EmptyMetadata())
    )(
      ifExisting = (existingStorageManifest: StorageManifest, existingMetadata: EmptyMetadata) =>
        (ifExisting(existingStorageManifest: StorageManifest), existingMetadata: EmptyMetadata)
    )
      .map { _ => () }
}
