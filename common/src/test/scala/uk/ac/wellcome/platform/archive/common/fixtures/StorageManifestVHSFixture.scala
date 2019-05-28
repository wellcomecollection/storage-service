package uk.ac.wellcome.platform.archive.common.fixtures

import org.scalatest.EitherValues
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestVHS
import uk.ac.wellcome.storage.StorageError
import uk.ac.wellcome.storage.memory.{MemoryObjectStore, MemoryVersionedDao}
import uk.ac.wellcome.storage.vhs.{EmptyMetadata, Entry, VersionedHybridStore}

import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.storage.streaming.CodecInstances._


trait StorageManifestVHSFixture extends EitherValues {
  type Dao = MemoryVersionedDao[String, Entry[String, EmptyMetadata]]
  type Store = MemoryObjectStore[StorageManifest]

  def createStore: Store = new MemoryObjectStore[StorageManifest]()
  def createDao: Dao = MemoryVersionedDao[String, Entry[String, EmptyMetadata]]
  def createStorageManifestVHS(dao: Dao, store: Store): StorageManifestVHS =
    new StorageManifestVHS(
      new VersionedHybridStore[String, StorageManifest, EmptyMetadata] {
        override protected val versionedDao: Dao = dao
        override protected val objectStore: Store = store
      })

  def storeSingleManifest(vhs: StorageManifestVHS,
                          storageManifest: StorageManifest): Either[StorageError, Unit] =
    vhs.updateRecord(
      ifNotExisting = storageManifest
    )(
      ifExisting = _ => throw new RuntimeException("VHS should be empty!")
    )

  def getStorageManifest(dao: Dao, store: Store, id: BagId): StorageManifest = {
    val entry: Entry[String, EmptyMetadata] =
      dao.entries(id.toString)

    store.get(entry.location).right.value
  }
}
