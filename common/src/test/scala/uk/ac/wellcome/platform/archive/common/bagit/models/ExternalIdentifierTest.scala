package uk.ac.wellcome.platform.archive.common.bagit.models

import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ExternalIdentifierTest extends AnyFunSpec with Matchers {
  it("allows creating an external identifier") {
    ExternalIdentifier("b12345678")
  }

  it("allows creating an external identifier with slashes") {
    ExternalIdentifier("PP/MIA/1")
  }

  it("blocks creating an external identifier with an empty string") {
    assertFailsRequirement(
      identifier = "",
      message = "External identifier cannot be empty"
    )
  }

  it("blocks creating an external identifier that begins or ends with a slash") {
    assertFailsRequirement(
      identifier = "PP/MIA/",
      message = "External identifier cannot end with a slash"
    )

    assertFailsRequirement(
      identifier = "/PP/MIA",
      message = "External identifier cannot start with a slash"
    )
  }

  it("blocks creating an external identifier with consecutive slashes") {
    assertFailsRequirement(
      identifier = "PP//MIA",
      message = "External identifier cannot contain consecutive slashes"
    )
  }

  it("blocks creating an external identifier that ends with /versions") {
    assertFailsRequirement(
      identifier = "b12345678/versions",
      message = "External identifier cannot end with /versions"
    )
  }

  it("blocks creating an external identifier that contains a /vNN-like string") {
    assertFailsRequirement(
      identifier = "b12345678/v1",
      message = "External identifier cannot end with a version string"
    )

    assertFailsRequirement(
      identifier = "b12345678/v0",
      message = "External identifier cannot end with a version string"
    )

    assertFailsRequirement(
      identifier = "b12345678/v200",
      message = "External identifier cannot end with a version string"
    )

    assertFailsRequirement(
      identifier = "v2/a",
      message = "External identifier cannot start with a version string"
    )

    assertFailsRequirement(
      identifier = "b12345678/v2/a",
      message = "External identifier cannot contain a version string"
    )
  }

  private def assertFailsRequirement(
    identifier: String,
    message: String
  ): Assertion = {
    val err = intercept[IllegalArgumentException] {
      ExternalIdentifier(identifier)
    }

    err.getMessage shouldBe s"requirement failed: $message"
  }
}
