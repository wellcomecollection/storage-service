package uk.ac.wellcome.platform.archive.common.storage.models

import org.scalatest.{FunSpec, Matchers}

class StorageSpaceTest extends FunSpec with Matchers {
  it("allows creating a storage space") {
    StorageSpace("digitised")
  }

  it("blocks creating a storage space with a slash") {
    val err = intercept[IllegalArgumentException] {
      StorageSpace("alfa/bravo")
    }

    err.getMessage should startWith("requirement failed: Storage space cannot contain slashes")
  }

  it("blocks creating a storage space with a slash using .copy()") {
    val space = StorageSpace("digitised")

    val err = intercept[IllegalArgumentException] {
      space.copy(underlying = "alfa/bravo")
    }

    err.getMessage should startWith("requirement failed: Storage space cannot contain slashes")
  }

  it("blocks creating a storage space with an empty string") {
    val err = intercept[IllegalArgumentException] {
      StorageSpace("")
    }

    err.getMessage shouldBe "requirement failed: Storage space cannot be empty"
  }
}
