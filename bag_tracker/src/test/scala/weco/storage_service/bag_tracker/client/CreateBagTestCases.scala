package weco.storage_service.bag_tracker.client

import org.scalatest.EitherValues
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import weco.storage_service.bag_tracker.storage.memory.MemoryStorageManifestDao
import weco.storage_service.bagit.models.BagId
import weco.storage_service.generators.StorageManifestGenerators
import weco.storage_service.storage.models.StorageManifest
import weco.storage.store.memory.MemoryVersionedStore
import weco.storage.{RetryableError, StoreWriteError, WriteError}

trait CreateBagTestCases
    extends AnyFunSpec
    with EitherValues
    with ScalaFutures
    with StorageManifestGenerators
    with BagTrackerClientTestBase {
  describe("createBag") {
    it("stores a bag in the storage manifest dao") {
      val manifest = createStorageManifest

      withStorageManifestDao(initialManifests = Seq.empty) { dao =>
        withApi(dao) { _ =>
          withClient(trackerHost) { client =>
            val future = client.createBag(manifest)

            whenReady(future) { result =>
              result shouldBe Right(())

              dao
                .get(id = manifest.id, version = manifest.version)
                .value shouldBe manifest
            }
          }
        }
      }
    }

    it("returns a Left[BagTrackerCreateError] if it cannot store the bag") {
      val versionedStore =
        MemoryVersionedStore[BagId, StorageManifest](initialEntries = Map.empty)

      val brokenDao = new MemoryStorageManifestDao(versionedStore) {
        override def put(
          storageManifest: StorageManifest
        ): Either[WriteError, StorageManifest] =
          Left(StoreWriteError(new Throwable("BOOM!")))
      }

      withApi(brokenDao) { _ =>
        withClient(trackerHost) { client =>
          val future = client.createBag(createStorageManifest)

          whenReady(future) {
            _.left.value shouldBe a[BagTrackerCreateError]
          }
        }
      }
    }

    it("returns a retryable error if the tracker API is unavailable") {
      withApi() { _ =>
        withClient("http://localhost.nope:8080") { client =>
          val future = client.createBag(createStorageManifest)

          whenReady(future) { result =>
            val err = result.left.value
            err shouldBe a[BagTrackerCreateError]
            err shouldBe a[RetryableError]
          }
        }
      }
    }
  }
}
