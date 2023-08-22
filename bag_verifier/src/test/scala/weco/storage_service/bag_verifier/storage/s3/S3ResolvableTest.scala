package weco.storage_service.bag_verifier.storage.s3

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.storage.providers.s3.S3ObjectLocation

import java.net.URI

class S3ResolvableTest extends AnyFunSpec with Matchers {
  val resolver = new S3Resolvable()

  it("encodes an S3 URI") {
    val location = S3ObjectLocation(bucket = "example-bucket", key = "key.txt")
    val uri = resolver.resolve(location)

    uri shouldBe new URI("s3://example-bucket/key.txt")
  }

  it("encodes a space in the URI") {
    val location = S3ObjectLocation(bucket = "example-bucket", key = "key with spaces.txt")
    val uri = resolver.resolve(location)

    uri shouldBe new URI("s3://example-bucket/key%20with%20spaces.txt")
  }
}
