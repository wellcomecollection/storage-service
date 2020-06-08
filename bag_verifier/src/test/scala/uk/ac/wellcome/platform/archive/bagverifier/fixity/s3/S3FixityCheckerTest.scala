package uk.ac.wellcome.platform.archive.bagverifier.fixity.s3

import java.net.URI

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagverifier.fixity.{
  FixityChecker,
  FixityCheckerTestCases,
  FixityCouldNotRead
}
import uk.ac.wellcome.platform.archive.common.storage.services.S3Resolvable
import uk.ac.wellcome.platform.archive.common.storage.{LocationError, LocationNotFound}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.store.fixtures.BucketNamespaceFixtures

class S3FixityCheckerTest
    extends FixityCheckerTestCases[Bucket, Unit]
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

  override def withFixityChecker[R](
    testWith: TestWith[FixityChecker, R]
  )(implicit context: Unit): R =
    testWith(new S3FixityChecker())

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
      withFixityChecker {
        _.verify(verifiableLocation)
      }

    result shouldBe a[FixityCouldNotRead]

    val fixityCouldNotRead = result.asInstanceOf[FixityCouldNotRead]

    fixityCouldNotRead.verifiableLocation shouldBe verifiableLocation
    fixityCouldNotRead.e shouldBe a[LocationNotFound[_]]
    fixityCouldNotRead.e.getMessage should include(
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
      withFixityChecker {
        _.verify(verifiableLocation)
      }

    result shouldBe a[FixityCouldNotRead]

    val fixityCouldNotRead = result.asInstanceOf[FixityCouldNotRead]

    fixityCouldNotRead.verifiableLocation shouldBe verifiableLocation
    fixityCouldNotRead.e shouldBe a[LocationError[_]]
    fixityCouldNotRead.e.getMessage should include(
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
        withFixityChecker {
          _.verify(verifiableLocation)
        }

      result shouldBe a[FixityCouldNotRead]

      val fixityCouldNotRead = result.asInstanceOf[FixityCouldNotRead]

      fixityCouldNotRead.verifiableLocation shouldBe verifiableLocation
      fixityCouldNotRead.e shouldBe a[LocationNotFound[_]]
      fixityCouldNotRead.e.getMessage should include(
        "Location not available!"
      )
    }
  }
}
