package weco.storage_service.bag_verifier.storage.s3

import org.scalatest.{Assertion, EitherValues}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.storage.providers.s3.S3ObjectLocation

import java.net.URI

class S3LocatableResolvableTest extends AnyFunSpec with Matchers with EitherValues {
  val resolver = new S3Resolvable()

  it("round-trips an S3 location") {
    assertLocationIsRoundtrippedCorrectly(
      location = S3ObjectLocation(bucket = "example-bucket", key = "key.txt")
    )
  }

  it("round-trips an S3 URI") {
    assertUriIsRoundtrippedCorrectly(
      uri = new URI("s3://example-bucket/key.txt")
    )
  }

  it("round-trips an S3 location with a space in the key") {
    assertLocationIsRoundtrippedCorrectly(
      location = S3ObjectLocation(bucket = "example-bucket", key = "key with spaces.txt")
    )
  }

  it("round-trips an S3 URI with spaces in the key") {
    assertUriIsRoundtrippedCorrectly(
      uri = new URI("s3://example-bucket/key%20with%20spaces.txt")
    )
  }

  def assertUriIsRoundtrippedCorrectly(uri: URI): Assertion {
    val encodedLocation = S3Locatable.s3UriLocatable.locate(uri)(maybeRoot = None)
    val decodedUri = resolver.resolve(encodedLocation.value)

    decodedUri shouldBe uri
  }

  def assertLocationIsRoundtrippedCorrectly(location: S3ObjectLocation): Assertion {
    val encodedUri = resolver.resolve(location)
    val decodedLocation = S3Locatable.s3UriLocatable.locate(uri)(maybeRoot = None)

    decodedLocation.value shouldBe location
  }
}
