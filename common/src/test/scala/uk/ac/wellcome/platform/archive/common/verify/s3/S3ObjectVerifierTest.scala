package uk.ac.wellcome.platform.archive.common.verify.s3

import java.net.URI

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.storage.{
  LocationError,
  LocationNotFound
}
import uk.ac.wellcome.platform.archive.common.storage.services.S3Resolvable
import uk.ac.wellcome.platform.archive.common.verify._
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.store.fixtures.BucketNamespaceFixtures

class S3ObjectVerifierTest
    extends VerifierTestCases[Bucket, Unit]
    with BucketNamespaceFixtures {
  override def withContext[R](testWith: TestWith[Unit, R]): R =
    testWith(())

  override def putString(location: ObjectLocation, contents: String)(
    implicit context: Unit
  ): Unit =
    s3Client.putObject(
      location.namespace,
      location.path,
      contents
    )

  override def withVerifier[R](
    testWith: TestWith[Verifier, R]
  )(implicit context: Unit): R =
    testWith(new S3ObjectVerifier())

  implicit val context: Unit = ()

  implicit val s3Resolvable: S3Resolvable = new S3Resolvable()

  import uk.ac.wellcome.platform.archive.common.storage.Resolvable._

  override def resolve(location: ObjectLocation): URI =
    location.resolve

  it("fails if the bucket doesn't exist") {
    val checksum = randomChecksum

    val location = createObjectLocationWith(bucket = createBucket)

    val verifiableLocation = createVerifiableLocationWith(
      location = location,
      checksum = checksum
    )

    val result =
      withVerifier {
        _.verify(verifiableLocation)
      }

    result shouldBe a[VerifiedFailure]

    val verifiedFailure = result.asInstanceOf[VerifiedFailure]

    verifiedFailure.verifiableLocation shouldBe verifiableLocation
    verifiedFailure.e shouldBe a[LocationNotFound[_]]
    verifiedFailure.e.getMessage should include(
      "Location not available!"
    )
  }

  it("fails if the bucket name is invalid") {
    val checksum = randomChecksum

    val location = createObjectLocationWith(namespace = "ABCD")

    val verifiableLocation = createVerifiableLocationWith(
      location = location,
      checksum = checksum
    )

    val result =
      withVerifier {
        _.verify(verifiableLocation)
      }

    result shouldBe a[VerifiedFailure]

    val verifiedFailure = result.asInstanceOf[VerifiedFailure]

    verifiedFailure.verifiableLocation shouldBe verifiableLocation
    verifiedFailure.e shouldBe a[LocationError[_]]
    verifiedFailure.e.getMessage should include(
      "The specified bucket is not valid"
    )
  }

  it("fails if the key doesn't exist in the bucket") {
    withLocalS3Bucket { bucket =>
      val checksum = randomChecksum

      val location = createObjectLocationWith(bucket)

      val verifiableLocation = createVerifiableLocationWith(
        location = location,
        checksum = checksum
      )

      val result =
        withVerifier {
          _.verify(verifiableLocation)
        }

      result shouldBe a[VerifiedFailure]

      val verifiedFailure = result.asInstanceOf[VerifiedFailure]

      verifiedFailure.verifiableLocation shouldBe verifiableLocation
      verifiedFailure.e shouldBe a[LocationNotFound[_]]
      verifiedFailure.e.getMessage should include(
        "Location not available!"
      )
    }
  }
}
