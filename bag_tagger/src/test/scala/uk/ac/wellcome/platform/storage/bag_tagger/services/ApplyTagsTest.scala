package uk.ac.wellcome.platform.storage.bag_tagger.services

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ApplyTagsTest extends AnyFunSpec with Matchers {
  describe("it applies tags") {
    it("to objects in S3") {
      true shouldBe false
    }
  }

  describe("it returns an error") {
    it("if asked to tag a non-existent object") {
      true shouldBe false
    }

    it("if asked to tag objects in Azure") {
      true shouldBe false
    }

    it("if tagging some of the objects fails") {
      true shouldBe false
    }

    it("if the objects have not been tagged by the verifier") {
      true shouldBe false
    }
  }
}
