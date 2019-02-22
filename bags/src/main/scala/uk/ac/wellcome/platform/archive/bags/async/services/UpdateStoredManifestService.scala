package uk.ac.wellcome.platform.archive.bags.async.services

import uk.ac.wellcome.platform.archive.common.models.StorageManifest
import uk.ac.wellcome.storage.ObjectStore
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.storage.vhs.{EmptyMetadata, VersionedHybridStore}

import scala.concurrent.{ExecutionContext, Future}

class UpdateStoredManifestService(
  vhs: VersionedHybridStore[StorageManifest,
                            EmptyMetadata,
                            ObjectStore[StorageManifest]]
)(implicit ec: ExecutionContext) {
  def updateStoredManifest(
    storageManifest: StorageManifest): Future[Unit] =
    vhs
      .updateRecord(storageManifest.id.toString)(
        ifNotExisting = (storageManifest, EmptyMetadata()))(
        ifExisting = (_, _) => (storageManifest, EmptyMetadata())
      )
      .map { _ => () }
}
