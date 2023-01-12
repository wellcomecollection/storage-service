package weco.storage_service.bag_verifier.storage.s3

import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.storage.s3.S3ObjectLocation
import weco.storage_service.bag_verifier.storage.LocationParsingError

import java.net.URI

class S3LocatableTest extends AnyFunSpec with Matchers with EitherValues {
  it("decodes an S3 URI") {
    val uri = new URI("s3://example-bucket/key.txt")

    S3Locatable.s3UriLocatable.locate(uri)(maybeRoot = None) shouldBe Right(
      S3ObjectLocation(bucket = "example-bucket", key = "key.txt")
    )
  }

  it("decodes a percent-encoded space in a URI") {
    val uri = new URI("s3://example-bucket/key%20with%20spaces.txt")

    S3Locatable.s3UriLocatable.locate(uri)(maybeRoot = None) shouldBe Right(
      S3ObjectLocation(bucket = "example-bucket", key = "key with spaces.txt")
    )
  }

  it("decodes an HTTP URL") {
    val uri = new URI("http://localhost:33333/example-bucket/key.txt")

    S3Locatable.s3UriLocatable.locate(uri)(maybeRoot = None) shouldBe Right(
      S3ObjectLocation(bucket = "example-bucket", key = "key.txt")
    )
  }

  it("fails if it cannot decode the URL") {
    val uri = new URI("https://example.com/cat.jpg")

    S3Locatable.s3UriLocatable.locate(uri)(maybeRoot = None).left.value shouldBe a[LocationParsingError[_]]
  }
}
