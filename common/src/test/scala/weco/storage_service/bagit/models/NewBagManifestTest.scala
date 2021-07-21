package weco.storage_service.bagit.models

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Try

class NewBagManifestTest extends AnyFunSpec with Matchers {
  it("allows an empty payload manifest") {
    NewPayloadManifest(entries = Map())
  }

  it("blocks an empty tag manifest") {
    val err = Try {
      NewTagManifest(entries = Map())
    }

    err.failed.get shouldBe a[IllegalArgumentException]
  }
}
