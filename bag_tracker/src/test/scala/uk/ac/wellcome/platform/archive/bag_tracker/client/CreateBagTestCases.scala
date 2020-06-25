package uk.ac.wellcome.platform.archive.bag_tracker.client

import org.scalatest.EitherValues
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.storage.services.memory.MemoryStorageManifestDao
import uk.ac.wellcome.storage.store.memory.MemoryVersionedStore
import uk.ac.wellcome.storage.{StoreWriteError, WriteError}

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
                .right
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

    it("fails if the tracker API is unavailable") {
      withApi() { _ =>
        withClient("http://localhost.nope:8080") { client =>
          val future = client.createBag(createStorageManifest)

          whenReady(future.failed) {
            _ shouldBe a[Throwable]
          }
        }
      }
    }
  }
}
