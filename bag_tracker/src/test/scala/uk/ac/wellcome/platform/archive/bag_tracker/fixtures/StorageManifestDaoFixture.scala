package uk.ac.wellcome.platform.archive.bag_tracker.fixtures

import org.scalatest.EitherValues
import weco.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bag_tracker.storage.StorageManifestDao
import uk.ac.wellcome.platform.archive.bag_tracker.storage.memory.MemoryStorageManifestDao
import weco.storage_service.bagit.models.BagId
import weco.storage_service.storage.models.StorageManifest
import uk.ac.wellcome.storage.Version
import uk.ac.wellcome.storage.maxima.memory.MemoryMaxima
import weco.storage.store.memory._

trait StorageManifestDaoFixture extends EitherValues {
  type StorageManifestIndex =
    MemoryStore[Version[String, Int], String] with MemoryMaxima[String, String]

  type StorageManifestTypedStore = MemoryTypedStore[String, StorageManifest]

  def createStorageManifestIndex: StorageManifestIndex =
    new MemoryStore[Version[String, Int], String](
      initialEntries = Map.empty
    ) with MemoryMaxima[String, String]

  def createTypedStore: StorageManifestTypedStore =
    MemoryTypedStore[String, StorageManifest](initialEntries = Map.empty)

  def createStorageManifestDao(): StorageManifestDao =
    new MemoryStorageManifestDao(
      MemoryVersionedStore[BagId, StorageManifest](
        initialEntries = Map.empty
      )
    )
}
