package uk.ac.wellcome.platform.archive.bag_tracker.client

import org.scalatest.EitherValues
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, BagVersion}
import uk.ac.wellcome.platform.archive.common.generators.{
  BagIdGenerators,
  StorageManifestGenerators
}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.storage.services.EmptyMetadata
import uk.ac.wellcome.platform.archive.common.storage.services.memory.MemoryStorageManifestDao
import uk.ac.wellcome.storage.{ReadError, StoreReadError}
import uk.ac.wellcome.storage.store.memory.MemoryVersionedStore

trait GetLatestBagTestCases
    extends AnyFunSpec
    with EitherValues
    with ScalaFutures
    with BagIdGenerators
    with BagTrackerClientTestBase
    with StorageManifestGenerators {

  describe("getLatestBag()") {
    it("finds the latest version of a bag") {
      val space = createStorageSpace
      val externalIdentifier = createExternalIdentifier

      val manifests = (1 to 5).map { version =>
        createStorageManifestWith(
          space = space,
          bagInfo = createBagInfoWith(externalIdentifier = externalIdentifier),
          version = BagVersion(version)
        )
      }

      val bagId = BagId(space = space, externalIdentifier = externalIdentifier)

      withApi(initialManifests = manifests) { _ =>
        withClient(trackerHost) { client =>
          val future = client.getLatestBag(bagId = bagId)

          whenReady(future) {
            _.right.value shouldBe manifests.last
          }
        }
      }
    }

    it("returns a Left[BagTrackerNotFoundError] if the bag does not exist") {
      withApi() { _ =>
        withClient(trackerHost) { client =>
          val future = client.getLatestBag(bagId = createBagId)

          whenReady(future) {
            _.left.value shouldBe BagTrackerNotFoundError()
          }
        }
      }
    }

    it(
      "returns a Left[BagTrackerUnknownGetError] if the API has an unexpected error"
    ) {
      val versionedStore =
        MemoryVersionedStore[BagId, StorageManifest](initialEntries = Map.empty)

      val brokenDao = new MemoryStorageManifestDao(versionedStore) {
        override def getLatest(id: BagId): Either[ReadError, StorageManifest] =
          Left(StoreReadError(new Throwable("BOOM!")))
      }

      withApi(brokenDao) { _ =>
        withClient(trackerHost) { client =>
          val future = client.getLatestBag(bagId = createBagId)

          whenReady(future) {
            _.left.value shouldBe a[BagTrackerUnknownGetError]
          }
        }
      }
    }

    it("fails if the tracker API is unavailable") {
      withApi() { _ =>
        withClient("http://localhost.nope:8080") { client =>
          val future = client.getLatestBag(bagId = createBagId)

          whenReady(future.failed) {
            _ shouldBe a[Throwable]
          }
        }
      }
    }
  }
}
