package uk.ac.wellcome.platform.archive.common.fixtures

import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestVHS
import uk.ac.wellcome.storage.memory.{MemoryObjectStore, MemoryVersionedDao}
import uk.ac.wellcome.storage.vhs.{EmptyMetadata, Entry, VersionedHybridStore}
import uk.ac.wellcome.storage.{ObjectStore, VersionedDao}

import scala.util.Try

trait StorageManifestVHSFixture {
  def createStorageManifestDao: MemoryVersionedDao[String, Entry[String, EmptyMetadata]] = MemoryVersionedDao[String, Entry[String, EmptyMetadata]]()

  def createStorageManifestStore: MemoryObjectStore[StorageManifest] = new MemoryObjectStore[StorageManifest]()

  def createStorageManifestVHS(
    dao: VersionedDao[String, Entry[String, EmptyMetadata]] = createStorageManifestDao,
    store: ObjectStore[StorageManifest] = createStorageManifestStore
  ): StorageManifestVHS = {
    val underlying = new VersionedHybridStore[String, StorageManifest, EmptyMetadata] {
      override protected val versionedDao: VersionedDao[String, VHSEntry] = dao
      override protected val objectStore: ObjectStore[StorageManifest] = store
    }

    new StorageManifestVHS(underlying)
  }

  def storeSingleManifest(vhs: StorageManifestVHS,
                          storageManifest: StorageManifest): Try[Unit] =
    vhs.updateRecord(
      ifNotExisting = storageManifest
    )(
      ifExisting = _ => throw new RuntimeException("VHS should be empty!")
    )
}
