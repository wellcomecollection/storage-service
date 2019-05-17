package uk.ac.wellcome.platform.archive.common.bagit.models

import org.scalatest.{FunSpec, Matchers}

class BagItemPathTest extends FunSpec with Matchers {

  it("can be created") {
    BagPath("bag-info.txt").value shouldBe "bag-info.txt"
  }

  it("can be created with an optional root path") {
    BagPath("bag-info.txt", Some("bag")).value shouldBe "bag/bag-info.txt"
  }

  it("normalises item path when joining paths") {
    BagPath("/bag-info.txt", Some("bag")).value shouldBe "bag/bag-info.txt"
  }

  it("normalises root path when joining paths") {
    BagPath("bag-info.txt", Some("bag/")).value shouldBe "bag/bag-info.txt"
  }

}
