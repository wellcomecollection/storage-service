package uk.ac.wellcome.platform.archive.common.storage.models

import org.scalatest.{Assertion, FunSpec, Matchers}

class StorageSpaceTest extends FunSpec with Matchers {
  it("allows creating a storage space") {
    StorageSpace("digitised")
  }

  it("blocks creating a storage space with a slash") {
    assertFailsRequirement(
      space = "alfa/bravo",
      message = "Storage space cannot contain slashes"
    )
  }

  it("blocks creating a storage space with an empty string") {
    assertFailsRequirement(
      space = "",
      message = "Storage space cannot be empty"
    )
  }

  // When we construct the S3 key for a bag, it's typically of the form
  //
  //    s3://{bucket}/{space}/{externalIdentifier}
  //
  // So if the space began with a slash (say /digitised), the key would be
  //
  //    s3://{bucket}//digitised/{externalIdentifier}
  //
  // And the S3 Console is liable to do weird things with double slashes
  // in the key.
  it("blocks creating a storage sapce that begins or ends with a slash") {
    assertFailsRequirement(
      space = "digitised/",
      message = "Storage space cannot contain slashes"
    )

    assertFailsRequirement(
      space = "/digitised",
      message = "Storage space cannot contain slashes"
    )
  }

  private def assertFailsRequirement(space: String, message: String): Assertion = {
    val err = intercept[IllegalArgumentException] {
      StorageSpace(space)
    }

    err.getMessage should startWith(s"requirement failed: $message")
  }
}
