package weco.storage_service.bag_verifier.storage.s3

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.storage.s3.S3ObjectLocation

import java.net.URI

class S3LocatableTest extends AnyFunSpec with Matchers {
  it("decodes a percent-encoded space in a URI") {
    val uri = new URI("s3://example-bucket/key%20with%20spaces.txt")

    S3Locatable.s3UriLocatable.locate(uri)(maybeRoot = None) shouldBe Right(
      S3ObjectLocation(bucket = "example-bucket", key = "key with spaces.txt")
    )
  }
}
