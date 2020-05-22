package uk.ac.wellcome.platform.archive.bag_tracker.client

import org.scalatest.EitherValues
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import uk.ac.wellcome.platform.archive.common.generators.{BagIdGenerators, StorageManifestGenerators}

trait GetLatestBagTestCases extends AnyFunSpec
  with EitherValues
  with ScalaFutures
  with BagIdGenerators
  with BagTrackerClientTestBase
  with StorageManifestGenerators {

  describe("getLatestBag()") {
    it("finds the latest version of a bag") {
      true shouldBe false
    }

    it("returns a Left[BagTrackerNotFoundError] if the bag does not exist") {
      true shouldBe false
    }

    it("returns a Left[BagTrackerUnknownGetError] if the API has an unexpected error") {
      true shouldBe false
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
