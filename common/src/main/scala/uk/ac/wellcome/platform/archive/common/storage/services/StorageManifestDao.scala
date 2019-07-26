package uk.ac.wellcome.platform.archive.common.storage.services

import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, BagVersion}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.store.{HybridStoreEntry, VersionedStore}

case class EmptyMetadata()

trait StorageManifestDao {
  val vhs: VersionedStore[BagId,
                          Int,
                          HybridStoreEntry[StorageManifest, EmptyMetadata]]

  def getLatest(id: BagId): Either[ReadError, StorageManifest] =
    vhs.getLatest(id).map { _.identifiedT.t }

  def get(id: BagId, version: BagVersion): Either[ReadError, StorageManifest] =
    vhs.get(Version(id, version.underlying)).map { _.identifiedT.t }

  def put(
    storageManifest: StorageManifest): Either[WriteError, StorageManifest] =
    vhs
      .put(id = Version(storageManifest.id, storageManifest.version.underlying))(
        HybridStoreEntry(storageManifest, metadata = EmptyMetadata())
      )
      .map { _.identifiedT.t }

  protected def listVersions(
    bagId: BagId,
    before: Option[BagVersion]): Either[ReadError, Seq[StorageManifest]]

  def listVersions(bagId: BagId): Either[ReadError, Seq[StorageManifest]] =
    listVersions(bagId, before = None)

  def listVersions(bagId: BagId,
                   before: BagVersion): Either[ReadError, Seq[StorageManifest]] =
    listVersions(bagId, before = Some(before))
}
