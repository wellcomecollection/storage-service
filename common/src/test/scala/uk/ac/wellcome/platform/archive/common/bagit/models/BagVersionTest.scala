package uk.ac.wellcome.platform.archive.common.bagit.models

import org.scalatest.TryValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class BagVersionTest extends AnyFunSpec with Matchers with TryValues {
  it("increments a version") {
    BagVersion(1).increment shouldBe BagVersion(2)
  }

  it("can cast a version to a string and back") {
    val version = BagVersion(5)

    version.toString shouldBe "v5"

    BagVersion.fromString(version.toString).success.value shouldBe version
  }

  it("fails to parse something that isn't a version") {
    val err = BagVersion.fromString("vX").failed.get

    err shouldBe a[Throwable]
    err.getMessage shouldBe "Could not parse version string: vX"
  }
}
