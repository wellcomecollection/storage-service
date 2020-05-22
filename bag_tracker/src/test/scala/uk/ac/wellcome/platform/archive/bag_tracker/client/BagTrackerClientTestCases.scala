package uk.ac.wellcome.platform.archive.bag_tracker.client

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

trait BagTrackerClientTestCases extends AnyFunSpec with Matchers {
  describe("listVersionsOf") {
    it("finds a single version of a bag") {
      true shouldBe false
    }

    it("finds multiple versions of a bag") {
      true shouldBe false
    }

    it("filters to versions before a given version") {
      true shouldBe false
    }

    it("returns Left[BagTrackerNotFoundListError] if there are no versions for this bag ID") {
      true shouldBe false
    }

    it("returns Left[BagTrackerNotFoundListError] if there are no versions before the given version") {
      true shouldBe false
    }

    it("returns Left[BagTrackerUnknownListError] if the API has an unexpected error") {
      true shouldBe false
    }
  }
}

class AkkaBagTrackerClientTest extends BagTrackerClientTestCases