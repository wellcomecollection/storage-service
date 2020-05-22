package uk.ac.wellcome.platform.archive.bag_tracker.client

import org.scalatest.funspec.AnyFunSpec

trait GetBagTestCases extends AnyFunSpec with BagTrackerClientTestBase {
  describe("getBag") {
    it("finds the correct version of a bag") {
      true shouldBe false
    }

    it("returns a Left[BagTrackerNotFoundError] if the bag does not exist") {
      true shouldBe false
    }

    it("returns a Left[BagTrackerUnknownGetError] if the API has an unexpected error") {
      true shouldBe false
    }

    it("fails if the tracker API is unavailable") {
      true shouldBe false
    }
  }
}
