package weco.storage_service.bag_tracker.client

import org.scalatest.EitherValues
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor1}
import weco.storage_service.bag_tracker.storage.memory.MemoryStorageManifestDao
import weco.storage_service.bagit.models.{BagId, BagVersion, ExternalIdentifier}
import weco.storage_service.generators.{
  BagIdGenerators,
  StorageManifestGenerators
}
import weco.storage_service.storage.models.StorageManifest
import weco.storage.{ReadError, RetryableError, StoreReadError}
import weco.storage.store.memory.MemoryVersionedStore

trait GetLatestBagTestCases
    extends AnyFunSpec
    with EitherValues
    with ScalaFutures
    with BagIdGenerators
    with BagTrackerClientTestBase
    with StorageManifestGenerators
    with TableDrivenPropertyChecks {

  val unusualIdentifiers: TableFor1[String]

  describe("getLatestBag()") {
    it("finds the latest version of a bag") {
      val space = createStorageSpace
      val externalIdentifier = createExternalIdentifier

      val manifests = (1 to 5).map { version =>
        createStorageManifestWith(
          space = space,
          externalIdentifier = externalIdentifier,
          version = BagVersion(version)
        )
      }

      val bagId = BagId(space = space, externalIdentifier = externalIdentifier)

      withApi(initialManifests = manifests) { _ =>
        withClient(trackerHost) { client =>
          val future = client.getLatestBag(bagId = bagId)

          whenReady(future) {
            _.value shouldBe manifests.last
          }
        }
      }
    }

    it("finds a bag with spaces in the identifier") {
      forAll(unusualIdentifiers) { identifier =>
        val manifest = createStorageManifestWith(
          externalIdentifier = ExternalIdentifier(identifier)
        )

        withApi(initialManifests = Seq(manifest)) { _ =>
          withClient(trackerHost) { client =>
            whenReady(client.getLatestBag(bagId = manifest.id)) {
              _.value shouldBe manifest
            }
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

    it("returns a retryable error if the tracker API is unavailable") {
      withApi() { _ =>
        withClient("http://localhost.nope:8080") { client =>
          val future = client.getLatestBag(bagId = createBagId)

          whenReady(future) { result =>
            val err = result.left.value
            err shouldBe a[BagTrackerUnknownGetError]
            err shouldBe a[RetryableError]
          }
        }
      }
    }
  }
}
