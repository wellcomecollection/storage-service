package uk.ac.wellcome.platform.archive.bag_tracker.client

import org.scalatest.EitherValues
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import uk.ac.wellcome.platform.archive.bag_tracker.storage.memory.MemoryStorageManifestDao
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, BagVersion}
import uk.ac.wellcome.platform.archive.common.generators.{
  BagIdGenerators,
  StorageManifestGenerators
}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.storage.{ReadError, StoreReadError}
import uk.ac.wellcome.storage.store.memory.MemoryVersionedStore

trait GetBagTestCases
    extends AnyFunSpec
    with EitherValues
    with ScalaFutures
    with BagIdGenerators
    with BagTrackerClientTestBase
    with StorageManifestGenerators {
  describe("getBag") {
    it("finds the correct version of a bag") {
      val space = createStorageSpace
      val externalIdentifier = createExternalIdentifier

      // Normally we don't assign a version 0, but this has the nice side-effect that
      // the manifest at index N has version N.
      val manifests = (0 to 5).map { version =>
        createStorageManifestWith(
          space = space,
          bagInfo = createBagInfoWith(externalIdentifier = externalIdentifier),
          version = BagVersion(version)
        )
      }

      val bagId = BagId(space = space, externalIdentifier = externalIdentifier)

      withApi(initialManifests = manifests) { _ =>
        withClient(trackerHost) { client =>
          val future = client.getBag(bagId = bagId, version = BagVersion(4))

          whenReady(future) {
            _.right.value shouldBe manifests(4)
          }
        }
      }
    }

    it("returns a Left[BagTrackerNotFoundError] if the bag does not exist") {
      val space = createStorageSpace
      val externalIdentifier = createExternalIdentifier

      val manifests = (1 to 3).map { version =>
        createStorageManifestWith(
          space = space,
          bagInfo = createBagInfoWith(externalIdentifier = externalIdentifier),
          version = BagVersion(version)
        )
      }

      val bagId = BagId(space = space, externalIdentifier = externalIdentifier)

      withApi(initialManifests = manifests) { _ =>
        withClient(trackerHost) { client =>
          val future = client.getBag(bagId = bagId, version = BagVersion(4))

          whenReady(future) {
            _.left.value shouldBe BagTrackerNotFoundError()
          }
        }
      }
    }

    it(
      "returns a Left[BagTrackerNotFoundError] if there are no versions of this bag"
    ) {
      withApi() { _ =>
        withClient(trackerHost) { client =>
          val future =
            client.getBag(bagId = createBagId, version = createBagVersion)

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
        override def get(
          id: BagId,
          version: BagVersion
        ): Either[ReadError, StorageManifest] =
          Left(StoreReadError(new Throwable("BOOM!")))
      }

      withApi(brokenDao) { _ =>
        withClient(trackerHost) { client =>
          val future =
            client.getBag(bagId = createBagId, version = createBagVersion)

          whenReady(future) {
            _.left.value shouldBe a[BagTrackerUnknownGetError]
          }
        }
      }
    }

    it("fails if the tracker API is unavailable") {
      withApi() { _ =>
        withClient("http://localhost.nope:8080") { client =>
          val future =
            client.getBag(bagId = createBagId, version = createBagVersion)

          whenReady(future.failed) {
            _ shouldBe a[Throwable]
          }
        }
      }
    }
  }
}
