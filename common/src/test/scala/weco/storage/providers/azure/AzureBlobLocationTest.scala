package weco.storage.providers.azure

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class AzureBlobLocationTest extends AnyFunSpec with Matchers {
  describe("AzureBlobLocation") {
    val loc = AzureBlobLocation(
      container = "my-azure-container",
      name = "path/to/pictures")

    it("joins paths") {
      loc.join("cats", "devon-rex.jpg") shouldBe AzureBlobLocation(
        container = "my-azure-container",
        name = "path/to/pictures/cats/devon-rex.jpg"
      )
    }

    it("removes double slashes when joining paths") {
      loc.join("trailing-slash/", "cornish-rex.jpg") shouldBe AzureBlobLocation(
        container = "my-azure-container",
        name = "path/to/pictures/trailing-slash/cornish-rex.jpg"
      )
    }

    it("creates a prefix") {
      loc.asPrefix shouldBe AzureBlobLocationPrefix(
        container = "my-azure-container",
        namePrefix = "path/to/pictures"
      )
    }

    it("casts to a string") {
      loc.toString shouldBe "azure://my-azure-container/path/to/pictures"
    }

    it("blocks the . and .. characters in the blob name") {
      val err1 = intercept[IllegalArgumentException] {
        AzureBlobLocation(
          container = "my-azure-container",
          name = "path/./to/pictures")
      }

      err1.getMessage shouldBe "requirement failed: Azure blob name cannot contain '.' or '..' entries: path/./to/pictures"

      val err2 = intercept[IllegalArgumentException] {
        AzureBlobLocation(
          container = "my-azure-container",
          name = "path/../to/pictures")
      }

      err2.getMessage shouldBe "requirement failed: Azure blob name cannot contain '.' or '..' entries: path/../to/pictures"
    }

    it("blocks multiple consecutive slashes") {
      val err = intercept[IllegalArgumentException] {
        AzureBlobLocation(
          container = "my-azure-container",
          name = "path//to/pictures")
      }

      err.getMessage shouldBe "requirement failed: Azure blob name cannot include multiple consecutive slashes: path//to/pictures"
    }

    it("blocks a trailing slash") {
      val err = intercept[IllegalArgumentException] {
        AzureBlobLocation(
          container = "my-azure-container",
          name = "path/to/cat.jpg/")
      }

      err.getMessage shouldBe "requirement failed: Azure blob name cannot end with a slash: path/to/cat.jpg/"
    }
  }

  describe("AzureBlobLocationPrefix") {
    val prefix = AzureBlobLocationPrefix(
      container = "my-azure-container",
      namePrefix = "path/to/different/pictures"
    )

    it("creates a location") {
      prefix.asLocation("dogs", "corgi.png") shouldBe AzureBlobLocation(
        container = "my-azure-container",
        name = "path/to/different/pictures/dogs/corgi.png"
      )
    }

    it("gets the basename") {
      prefix.basename shouldBe "pictures"
    }

    it("gets the parent") {
      prefix.parent shouldBe AzureBlobLocationPrefix(
        container = "my-azure-container",
        namePrefix = "path/to/different"
      )
    }

    it("casts to a string") {
      prefix.toString shouldBe "azure://my-azure-container/path/to/different/pictures"
    }

    it("blocks the . and .. characters in the blob name") {
      val err1 = intercept[IllegalArgumentException] {
        AzureBlobLocationPrefix(
          container = "my-azure-container",
          namePrefix = "path/./to/pictures")
      }

      err1.getMessage shouldBe "requirement failed: Azure blob name prefix cannot contain '.' or '..' entries: path/./to/pictures"

      val err2 = intercept[IllegalArgumentException] {
        AzureBlobLocationPrefix(
          container = "my-azure-container",
          namePrefix = "path/../to/pictures")
      }

      err2.getMessage shouldBe "requirement failed: Azure blob name prefix cannot contain '.' or '..' entries: path/../to/pictures"
    }

    it("blocks multiple consecutive slashes") {
      val err1 = intercept[IllegalArgumentException] {
        AzureBlobLocationPrefix(
          container = "my-azure-container",
          namePrefix = "path/to/pictures//")
      }

      err1.getMessage shouldBe "requirement failed: Azure blob name prefix cannot include multiple consecutive slashes: path/to/pictures//"

      val err2 = intercept[IllegalArgumentException] {
        AzureBlobLocationPrefix(
          container = "my-azure-container",
          namePrefix = "path//to/pictures")
      }

      err2.getMessage shouldBe "requirement failed: Azure blob name prefix cannot include multiple consecutive slashes: path//to/pictures"
    }

    it("allows a trailing slash") {
      AzureBlobLocationPrefix(
        container = "my-azure-container",
        namePrefix = "path/to/pictures/")
    }
  }
}
