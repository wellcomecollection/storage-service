package uk.ac.wellcome.platform.archive.common.bagit.models

import org.scalatest.{FunSpec, Matchers}

class ExternalIdentifierTest extends FunSpec with Matchers {
  it("allows creating an external identifier") {
    ExternalIdentifier("b12345678")
  }

  it("blocks creating an external identifier with an empty string") {
    val err = intercept[IllegalArgumentException] {
      ExternalIdentifier("")
    }

    err.getMessage shouldBe "requirement failed: External identifier cannot be empty"
  }

  it("allows creating an external identifier with slashes") {
    ExternalIdentifier("PP/MIA/1")
  }
}
