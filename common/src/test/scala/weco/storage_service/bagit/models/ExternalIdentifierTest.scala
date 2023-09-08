package weco.storage_service.bagit.models

import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ExternalIdentifierTest extends AnyFunSpec with Matchers {
  describe("permitted characters in identifiers") {
    info(
      "A permissible identifier consists of alphanumeric characters (Basic Latin only)" +
      "plus spaces, underscores, hyphens and forward slashes."
    )

    it("allows creating an alphanumeric external identifier") {
      ExternalIdentifier("b12345678")
    }

    it("allows creating an external identifier with spaces") {
      ExternalIdentifier("PP MIA 1")
    }

    it("allows creating an external identifier with slashes") {
      ExternalIdentifier("PP/MIA/1")
    }

    it("allows creating an external identifier with hyphens") {
      ExternalIdentifier("PP-MIA-1")
    }

    it("allows creating an external identifier with underscores") {
      ExternalIdentifier("PP_MIA_1")
    }

    it("allows creating an external identifier with dots") {
      ExternalIdentifier("PP.MIA.1")
    }
  }

  describe("forbidden characters in identifiers") {
    info(
      "the set of characters that may cause an identifier to be rejected is vast, " +
        "but here are a few examples that are likely to be encountered"
    )

    it("blocks creating an external identifier with non-English letters") {
      assertFailsRequirement(
        identifier = "PP/MIÅ/1",
        message = "External identifier must match regex: ^[-_/ .a-zA-Z0-9]+$"
      )
    }

    it("blocks creating an external identifier with common URL substitutions for spaces") {
      assertFailsRequirement(
        identifier = "miro+space",
        message = "External identifier must match regex: ^[-_/ .a-zA-Z0-9]+$"
      )

      assertFailsRequirement(
        identifier = "miro%20space",
        message = "External identifier must match regex: ^[-_/ .a-zA-Z0-9]+$"
      )
    }
  }

  describe("forbidden identifiers") {
    info(
      "In addition to the conforming to the permissible character set, " +
      "there are some further restrictions on the nature of an identifier."
    )

    it("blocks creating an external identifier with an empty string") {
      assertFailsRequirement(
        identifier = "",
        message = "External identifier cannot be empty"
      )
    }

    describe("start and end characters") {
      info("the first and last character in an identifier must be alphanumeric")

      it("blocks creating an external identifier with a leading non-alphanumeric character") {
        assertFailsRequirement(
          identifier = "-abc123",
          message = "External identifier must begin with a Basic Latin letter or digit"
        )
      }

      it("blocks creating an external identifier with a trailing non-alphanumeric character") {
        assertFailsRequirement(
          identifier = "abc123_",
          message = "External identifier must end with a Basic Latin letter or digit"
        )
      }
    }

    describe("restrictions around slashes") {
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
    }

    describe("restrictions around spaces") {
      it("blocks creating an external identifier with multiple consecutive spaces") {
        assertFailsRequirement(
          identifier = "PP  MIA",
          message = "External identifier cannot contain consecutive spaces"
        )
      }
    }

    describe("restrictions to prevent ambiguous URLs") {
      info("certain sequences are forbidden because they conflict with sequences used for versioning")

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
    }
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
