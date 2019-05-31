package uk.ac.wellcome.platform.archive.common.fixtures

import org.scalatest.EitherValues
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestDao
import uk.ac.wellcome.storage.StorageError
import uk.ac.wellcome.storage.memory.{MemoryObjectStore, MemoryVersionedDao}
import uk.ac.wellcome.storage.streaming.CodecInstances._
import uk.ac.wellcome.storage.vhs.{EmptyMetadata, Entry, VersionedHybridStore}

trait StorageManifestVHSFixture extends EitherValues {
  type StorageManifestVersionedDao =
    MemoryVersionedDao[String, Entry[String, EmptyMetadata]]
  type StorageManifestStore = MemoryObjectStore[StorageManifest]

  def createStore: StorageManifestStore =
    new MemoryObjectStore[StorageManifest]()
  def createDao: StorageManifestVersionedDao =
    MemoryVersionedDao[String, Entry[String, EmptyMetadata]]
  def createStorageManifestDao(
    dao: StorageManifestVersionedDao = createDao,
    store: StorageManifestStore = createStore): StorageManifestDao =
    new StorageManifestDao(
      new VersionedHybridStore[String, StorageManifest, EmptyMetadata] {
        override protected val versionedDao: StorageManifestVersionedDao = dao
        override protected val objectStore: StorageManifestStore = store
      })

  def storeSingleManifest(
    vhs: StorageManifestDao,
    storageManifest: StorageManifest): Either[StorageError, Unit] =
    vhs.update(
      ifNotExisting = storageManifest
    )(
      ifExisting = _ => throw new RuntimeException("VHS should be empty!")
    )

  def getStorageManifest(dao: StorageManifestVersionedDao,
                         store: StorageManifestStore,
                         id: BagId): StorageManifest = {
    val entry: Entry[String, EmptyMetadata] =
      dao.entries(id.toString)

    store.get(entry.location).right.value
  }
}
