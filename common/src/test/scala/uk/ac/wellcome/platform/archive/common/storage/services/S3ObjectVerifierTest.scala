package uk.ac.wellcome.platform.archive.common.storage.services

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.fixtures.VerifyFixtures
import uk.ac.wellcome.platform.archive.common.storage.LocationNotFound
import uk.ac.wellcome.platform.archive.common.verify._
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.store.fixtures.{BucketNamespaceFixtures, NamespaceFixtures}

trait BetterVerifierTestCases[Namespace, Context]
  extends FunSpec
    with Matchers
    with NamespaceFixtures[ObjectLocation, Namespace]
    with VerifyFixtures {

  def withContext[R](testWith: TestWith[Context, R]): R

  def createObjectLocationWith(namespace: Namespace): ObjectLocation

  def putString(location: ObjectLocation, contents: String)(implicit context: Context): Unit

  def withVerifier[R](testWith: TestWith[BetterVerifier[_], R])(implicit context: Context): R

  it("returns a success if the checksum is correct") {
    withContext { implicit context =>
      withNamespace { implicit namespace =>
        val contentHashingAlgorithm = MD5
        val contentString = "HelloWorld"
        // md5("HelloWorld")
        val contentStringChecksum = ChecksumValue(
          "68e109f0f40ca72a15e05cc22786f8e6"
        )
        val checksum = Checksum(contentHashingAlgorithm, contentStringChecksum)

        val location = createObjectLocationWith(namespace)
        putString(location, contentString)

        val verifiableLocation = createVerifiableLocationWith(
          location = location,
          checksum = checksum
        )

        val result =
          withVerifier {
            _.verify(verifiableLocation)
          }

        result shouldBe a[VerifiedSuccess]

        val verifiedSuccess = result.asInstanceOf[VerifiedSuccess]
        verifiedSuccess.location shouldBe verifiableLocation
      }
    }
  }

  it("fails if the object doesn't exist") {
    withContext { implicit context =>
      withNamespace { implicit namespace =>
        val checksum = randomChecksum

        val location = createObjectLocationWith(namespace)

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

        verifiedFailure.location shouldBe verifiableLocation
        verifiedFailure.e shouldBe a[LocationNotFound[_]]
        verifiedFailure.e.getMessage should include(
          "Location not available!"
        )
      }
    }
  }

  it("fails if the checksum is incorrect") {
    withContext { implicit context =>
      withNamespace { implicit namespace =>
        val checksum = randomChecksum

        val location = createObjectLocationWith(namespace)
        putString(location, randomAlphanumeric)

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

        verifiedFailure.location shouldBe verifiableLocation
        verifiedFailure.e shouldBe a[FailedChecksumNoMatch]
        verifiedFailure.e.getMessage should startWith(
          "Checksum values do not match"
        )
      }
    }
  }

  it("fails if the checksum is correct but the expected length is wrong") {
    withContext { implicit context =>
      withNamespace { implicit namespace =>
        val contentHashingAlgorithm = MD5
        val contentString = "HelloWorld"
        // md5("HelloWorld")
        val contentStringChecksum = ChecksumValue(
          "68e109f0f40ca72a15e05cc22786f8e6"
        )

        val location = createObjectLocationWith(namespace)
        val checksum = Checksum(contentHashingAlgorithm, contentStringChecksum)

        val verifiableLocation = createVerifiableLocationWith(
          location = location,
          checksum = checksum,
          length = Some(contentString.getBytes().length - 1)
        )

        putString(location, contentString)

        val result =
          withVerifier {
            _.verify(verifiableLocation)
          }

        result shouldBe a[VerifiedFailure]

        val verifiedFailure = result.asInstanceOf[VerifiedFailure]

        verifiedFailure.location shouldBe verifiableLocation
        verifiedFailure.e shouldBe a[Throwable]
        verifiedFailure.e.getMessage should startWith(
          "Lengths do not match:"
        )
      }
    }
  }

  it("succeeds if the checksum is correct and the lengths match") {
    withContext { implicit context =>
      withNamespace { implicit namespace =>
        val contentHashingAlgorithm = MD5
        val contentString = "HelloWorld"
        // md5("HelloWorld")
        val contentStringChecksum = ChecksumValue(
          "68e109f0f40ca72a15e05cc22786f8e6"
        )

        val location = createObjectLocationWith(namespace)
        val checksum = Checksum(contentHashingAlgorithm, contentStringChecksum)

        val verifiableLocation = createVerifiableLocationWith(
          location = location,
          checksum = checksum,
          length = Some(contentString.getBytes().length)
        )

        putString(location, contentString)

        val result =
          withVerifier {
            _.verify(verifiableLocation)
          }

        result shouldBe a[VerifiedSuccess]

        val verifiedSuccess = result.asInstanceOf[VerifiedSuccess]
        verifiedSuccess.location shouldBe verifiableLocation
      }
    }
  }

  it("supports different checksum algorithms") {
    withContext { implicit context =>
      withNamespace { implicit namespace =>
        val contentHashingAlgorithm = SHA256
        val contentString = "HelloWorld"
        // sha256("HelloWorld")
        val contentStringChecksum = ChecksumValue(
          "872e4e50ce9990d8b041330c47c9ddd11bec6b503ae9386a99da8584e9bb12c4"
        )

        val location = createObjectLocationWith(namespace)
        val checksum = Checksum(contentHashingAlgorithm, contentStringChecksum)

        val verifiableLocation = createVerifiableLocationWith(
          location = location,
          checksum = checksum
        )

        putString(location, contentString)

        val result =
          withVerifier {
            _.verify(verifiableLocation)
          }

        result shouldBe a[VerifiedSuccess]

        val verifiedSuccess = result.asInstanceOf[VerifiedSuccess]
        verifiedSuccess.location shouldBe verifiableLocation
      }
    }
  }
}

class BetterS3ObjectVerifierTest
  extends BetterVerifierTestCases[Bucket, Unit]
    with BucketNamespaceFixtures {
  override def withContext[R](testWith: TestWith[Unit, R]): R =
    testWith(())

  override def putString(location: ObjectLocation, contents: String)(implicit context: Unit): Unit =
    s3Client.putObject(
      location.namespace,
      location.path,
      contents
    )

  override def withVerifier[R](testWith: TestWith[BetterVerifier[_], R])(implicit context: Unit): R =
    testWith(new S3ObjectVerifier())

  implicit val context: Unit = ()

  it("fails if the bucket doesn't exist") {
    val checksum = randomChecksum

    val location = createObjectLocation

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

    verifiedFailure.location shouldBe verifiableLocation
    verifiedFailure.e shouldBe a[LocationNotFound[_]]
    verifiedFailure.e.getMessage should include(
      "Location not available!"
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

      verifiedFailure.location shouldBe verifiableLocation
      verifiedFailure.e shouldBe a[LocationNotFound[_]]
      verifiedFailure.e.getMessage should include(
        "Location not available!"
      )
    }
  }
}
