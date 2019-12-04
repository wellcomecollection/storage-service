package uk.ac.wellcome.platform.archive.common.bagit.models

import org.scalatest.FunSpec
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.json.utils.JsonAssertions
import uk.ac.wellcome.platform.archive.common.generators.BagIdGenerators
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace

class BagIdTest extends FunSpec with JsonAssertions with BagIdGenerators {
  it("serialises space and external identifier as strings") {
    val bagId =
      BagId(StorageSpace("digitised"), ExternalIdentifier("b1234567x"))
    val expectedJson =
      s"""
         |{
         |  "space": "digitised",
         |  "externalIdentifier": "b1234567x"
         |}
       """.stripMargin
    assertJsonStringsAreEqual(toJson(bagId).get, expectedJson)
  }

  it("can cast a BagId to a string and back again") {
    val bagId = createBagId

    BagId(bagId.toString) shouldBe bagId
  }

  it("can parse a slash in the string representation") {
    val bagId = BagId(
      space = StorageSpace("born-digital"),
      externalIdentifier = ExternalIdentifier("PP/MIA/1")
    )

    BagId(bagId.toString) shouldBe bagId
  }

  it("fails if you try to create a BagId from a string without a slash") {
    val err = intercept[IllegalArgumentException] {
      BagId("no slashes here")
    }

    err.getMessage shouldBe "Cannot create bag ID from no slashes here"
  }

  it(
    "fails if you try to create a BagId from a string with an empty storage space"
  ) {
    val err = intercept[IllegalArgumentException] {
      BagId("/b12345678")
    }

    err.getMessage shouldBe "requirement failed: Storage space cannot be empty"
  }

  it(
    "fails if you try to create a BagId from a string with an empty external identifier"
  ) {
    val err = intercept[IllegalArgumentException] {
      BagId("digitised/")
    }

    err.getMessage shouldBe "requirement failed: External identifier cannot be empty"
  }
}
