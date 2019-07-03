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
  type StorageManifestIndex =
    MemoryStore[Version[String, Int],
                HybridIndexedStoreEntry[Version[String, Int],
                                        String,
                                        Map[String, String]]] with MemoryMaxima[
      String,
      HybridIndexedStoreEntry[Version[String, Int], String, Map[String, String]]]

  type StorageManifestTypedStore = MemoryTypedStore[String, StorageManifest]

  def createIndex: StorageManifestIndex =
    new MemoryStore[
      Version[String, Int],
      HybridIndexedStoreEntry[Version[String, Int],
                              String,
                              Map[String, String]]](initialEntries = Map.empty)
    with MemoryMaxima[
      String,
      HybridIndexedStoreEntry[Version[String, Int], String, Map[String, String]]]

  def createTypedStore: StorageManifestTypedStore = {
    val memoryStoreForStreamStore =
      new MemoryStore[String, MemoryStreamStoreEntry](Map.empty)
    implicit val streamStore: MemoryStreamStore[String] =
      new MemoryStreamStore[String](memoryStoreForStreamStore)
    new MemoryTypedStore[String, StorageManifest](Map.empty)
  }

  def createStorageManifestDao(implicit
                               indexStore: StorageManifestIndex = createIndex,
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
