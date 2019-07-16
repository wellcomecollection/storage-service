package uk.ac.wellcome.platform.archive.common.storage.services

import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.store.{HybridStoreEntry, VersionedStore}

case class EmptyMetadata()

trait BetterStorageManifestDao {
  val vhs: VersionedStore[BagId, Int, HybridStoreEntry[StorageManifest, EmptyMetadata]]
  def getLatest(id: BagId): Either[ReadError, StorageManifest] =
    vhs.getLatest(id).map { _.identifiedT.t }

  def get(id: BagId, version: Int): Either[ReadError, StorageManifest] =
    vhs.get(Version(id, version)).map { _.identifiedT.t }

  def put(storageManifest: StorageManifest): Either[WriteError, StorageManifest] =
    vhs
      .put(id = Version(storageManifest.id, storageManifest.version))(
        HybridStoreEntry(storageManifest, metadata = EmptyMetadata())
      )
      .map { _.identifiedT.t }
}

// TODO: Do we need this wrapper at all now?
// TODO: This could be a Store!
class StorageManifestDao(
  val vhs: VersionedStore[
    String,
    Int,
    HybridStoreEntry[StorageManifest, Map[String, String]]]
) {
  def getLatest(id: BagId): Either[ReadError, StorageManifest] =
    vhs.getLatest(id.toString).map { _.identifiedT.t }

  def get(id: BagId, version: Int): Either[ReadError, StorageManifest] =
    vhs.get(Version(id.toString, version)).map { _.identifiedT.t }

  def put(
    storageManifest: StorageManifest): Either[WriteError, StorageManifest] =
    vhs
      .put(id = Version(storageManifest.id.toString, storageManifest.version))(
        HybridStoreEntry(storageManifest, metadata = Map("alex" -> "true"))
      )
      .map { _.identifiedT.t }
}
