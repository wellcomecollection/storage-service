package uk.ac.wellcome.platform.archive.common.fixtures

import org.scalatest.EitherValues
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestVHS
import uk.ac.wellcome.storage.StorageError
import uk.ac.wellcome.storage.memory.{MemoryObjectStore, MemoryVersionedDao}
import uk.ac.wellcome.storage.streaming.CodecInstances._
import uk.ac.wellcome.storage.vhs.{EmptyMetadata, Entry, VersionedHybridStore}

trait StorageManifestVHSFixture extends EitherValues {
  type StorageManifestDao =
    MemoryVersionedDao[String, Entry[String, EmptyMetadata]]
  type StorageManifestStore = MemoryObjectStore[StorageManifest]

  def createStore: StorageManifestStore =
    new MemoryObjectStore[StorageManifest]()
  def createDao: StorageManifestDao =
    MemoryVersionedDao[String, Entry[String, EmptyMetadata]]
  def createStorageManifestVHS(
    dao: StorageManifestDao = createDao,
    store: StorageManifestStore = createStore): StorageManifestVHS =
    new StorageManifestVHS(
      new VersionedHybridStore[String, StorageManifest, EmptyMetadata] {
        override protected val versionedDao: StorageManifestDao = dao
        override protected val objectStore: StorageManifestStore = store
      })

  def storeSingleManifest(
    vhs: StorageManifestVHS,
    storageManifest: StorageManifest): Either[StorageError, Unit] =
    vhs.updateRecord(
      ifNotExisting = storageManifest
    )(
      ifExisting = _ => throw new RuntimeException("VHS should be empty!")
    )

  def getStorageManifest(dao: StorageManifestDao,
                         store: StorageManifestStore,
                         id: BagId): StorageManifest = {
    val entry: Entry[String, EmptyMetadata] =
      dao.entries(id.toString)

    store.get(entry.location).right.value
  }
}
