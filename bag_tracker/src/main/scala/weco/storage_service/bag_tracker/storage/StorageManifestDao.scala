package weco.storage_service.bag_tracker.storage

import weco.storage_service.bagit.models.{BagId, BagVersion}
import weco.storage_service.storage.models.StorageManifest
import weco.storage.store.VersionedStore
import weco.storage.{HigherVersionExistsError, ReadError, Version, WriteError}

trait StorageManifestDao {
  val vhs: VersionedStore[
    BagId,
    Int,
    StorageManifest
  ]

  def getLatest(id: BagId): Either[ReadError, StorageManifest] =
    vhs.getLatest(id).map { _.identifiedT }

  def get(id: BagId, version: BagVersion): Either[ReadError, StorageManifest] =
    vhs.get(Version(id, version.underlying)).map { _.identifiedT }

  def put(
    storageManifest: StorageManifest
  ): Either[WriteError, StorageManifest] = {
    val id = Version(storageManifest.id, storageManifest.version.underlying)

    vhs.put(id)(storageManifest).map { _.identifiedT } match {
      // For storage manifests, it's okay to write versions out of order,
      // e.g. if SQS queues didn't deliver the messages correctly.
      // We care that manifests are immutable once written, not that previous
      // versions can never be written.
      //
      // e.g. once we've written the V2 manifest, it's still okay to write the
      // V1 manifest if it hasn't been written yet.
      case Left(_: HigherVersionExistsError) =>
        vhs.store.put(id)(storageManifest).map { _.identifiedT }

      case result => result
    }
  }

  def listVersions(
    bagId: BagId,
    before: Option[BagVersion]
  ): Either[ReadError, Seq[StorageManifest]]

  // TODO: The methods below should be replaced with uses of listVersions().

  def listAllVersions(bagId: BagId): Either[ReadError, Seq[StorageManifest]] =
    listVersions(bagId, before = None)

  def listVersionsBefore(
    bagId: BagId,
    before: BagVersion
  ): Either[ReadError, Seq[StorageManifest]] =
    listVersions(bagId, before = Some(before))
}
