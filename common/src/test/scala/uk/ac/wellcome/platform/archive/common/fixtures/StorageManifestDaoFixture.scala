package uk.ac.wellcome.platform.archive.common.fixtures

import org.scalatest.EitherValues
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestDao

import uk.ac.wellcome.platform.archive.common.storage.services.memory.MemoryStorageManifestDao
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.storage.Version
import uk.ac.wellcome.storage.maxima.memory.MemoryMaxima
import uk.ac.wellcome.storage.store.memory._

trait StorageManifestDaoFixture extends EitherValues {
  type StorageManifestIndex =
    MemoryStore[Version[String, Int], String] with MemoryMaxima[String, String]

  type StorageManifestTypedStore = MemoryTypedStore[String, StorageManifest]

  def createStorageManifestIndex: StorageManifestIndex =
    new MemoryStore[Version[String, Int], String](
      initialEntries = Map.empty
    ) with MemoryMaxima[String, String]

  def createTypedStore: StorageManifestTypedStore = {
    val memoryStoreForStreamStore =
      new MemoryStore[String, Array[Byte]](Map.empty)
    implicit val streamStore: MemoryStreamStore[String] =
      new MemoryStreamStore[String](memoryStoreForStreamStore)
    new MemoryTypedStore[String, StorageManifest](Map.empty)
  }

  def createStorageManifestDao(): StorageManifestDao =
    new MemoryStorageManifestDao(
      MemoryVersionedStore[BagId, StorageManifest](
        initialEntries = Map.empty
      )
    )
}
