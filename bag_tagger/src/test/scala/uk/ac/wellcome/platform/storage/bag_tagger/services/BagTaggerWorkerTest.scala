package uk.ac.wellcome.platform.storage.bag_tagger.services

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class BagTaggerWorkerTest extends AnyFunSpec with Matchers {
  describe("applies tags") {
    it("to a single location") {
      true shouldBe false
    }

    it("to every location on a bag") {
      true shouldBe false
    }
  }

  describe("handles errors") {
    it("if it can't read the version string as a bag version") {
      true shouldBe false
    }

    it("if it can't get the bag from the tracker") {
      true shouldBe false
    }

    it("if it can't apply the tags") {
      true shouldBe false
    }
  }
}
