package uk.ac.wellcome.platform.archive.common.fixtures

import org.scalatest.EitherValues
import uk.ac.wellcome.json.JsonUtil._
// import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestDao
import uk.ac.wellcome.storage.Version
import uk.ac.wellcome.storage.maxima.memory.MemoryMaxima
import uk.ac.wellcome.storage.store.HybridIndexedStoreEntry
import uk.ac.wellcome.storage.store.memory._

trait StorageManifestVHSFixture extends EitherValues {
  type StoreEntry = HybridIndexedStoreEntry[String, Map[String, String]]

  type StorageManifestIndex =
    MemoryStore[Version[String, Int], StoreEntry] with MemoryMaxima[String,
                                                                    StoreEntry]

  type StorageManifestTypedStore = MemoryTypedStore[String, StorageManifest]

  def createIndex: StorageManifestIndex =
    new MemoryStore[Version[String, Int], StoreEntry](
      initialEntries = Map.empty) with MemoryMaxima[String, StoreEntry]

  def createTypedStore: StorageManifestTypedStore = {
    val memoryStoreForStreamStore =
      new MemoryStore[String, MemoryStreamStoreEntry](Map.empty)
    implicit val streamStore: MemoryStreamStore[String] =
      new MemoryStreamStore[String](memoryStoreForStreamStore)
    new MemoryTypedStore[String, StorageManifest](Map.empty)
  }

  def createStorageManifestDao(implicit indexStore: StorageManifestIndex =
                                 createIndex,
                               typedStore: StorageManifestTypedStore =
                                 createTypedStore): StorageManifestDao =
    // TODO: This should use a companion object
    new StorageManifestDao(
      new MemoryVersionedHybridStore[
        String,
        StorageManifest,
        Map[String, String]](
        new MemoryHybridStoreWithMaxima[
          String,
          StorageManifest,
          Map[String, String]]()
      )
    )
}
