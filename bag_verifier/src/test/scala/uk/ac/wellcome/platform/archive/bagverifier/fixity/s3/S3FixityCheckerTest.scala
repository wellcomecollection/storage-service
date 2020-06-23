package uk.ac.wellcome.platform.archive.bagverifier.fixity.s3

import java.net.URI

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagverifier.fixity.{
  FileFixityCouldNotRead,
  FixityChecker,
  FixityCheckerTestCases
}
import uk.ac.wellcome.platform.archive.bagverifier.storage.Resolvable._
import uk.ac.wellcome.platform.archive.bagverifier.storage.s3.S3Resolvable
import uk.ac.wellcome.platform.archive.common.storage.{
  LocationError,
  LocationNotFound
}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.store.fixtures.BucketNamespaceFixtures
import uk.ac.wellcome.storage.store.s3.S3StreamStore

class S3FixityCheckerTest
    extends FixityCheckerTestCases[Bucket, Unit, S3StreamStore]
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

  override def withStreamStore[R](
    testWith: TestWith[S3StreamStore, R]
  )(implicit context: Unit): R =
    testWith(
      new S3StreamStore()
    )

  override def withFixityChecker[R](
    s3Store: S3StreamStore
  )(
    testWith: TestWith[FixityChecker, R]
  )(implicit context: Unit): R =
    testWith(new S3FixityChecker() {
      override val streamStore: StreamStore[ObjectLocation] = s3Store
    })

  implicit val context: Unit = ()

  implicit val s3Resolvable: S3Resolvable = new S3Resolvable()

  override def resolve(location: ObjectLocation): URI =
    location.resolve

  it("fails if the bucket doesn't exist") {
    val checksum = randomChecksum

    val location = createObjectLocationWith(bucket = createBucket)

    val expectedFileFixity = createExpectedFileFixityWith(
      location = location,
      checksum = checksum
    )

    val result =
      withFixityChecker {
        _.check(expectedFileFixity)
      }

    result shouldBe a[FileFixityCouldNotRead]

    val fixityCouldNotRead = result.asInstanceOf[FileFixityCouldNotRead]

    fixityCouldNotRead.expectedFileFixity shouldBe expectedFileFixity
    fixityCouldNotRead.e shouldBe a[LocationNotFound[_]]
    fixityCouldNotRead.e.getMessage should include(
      "Location not available!"
    )
  }

  it("fails if the bucket name is invalid") {
    val checksum = randomChecksum

    val location = createObjectLocationWith(namespace = "ABCD")

    val expectedFileFixity = createExpectedFileFixityWith(
      location = location,
      checksum = checksum
    )

    val result =
      withFixityChecker {
        _.check(expectedFileFixity)
      }

    result shouldBe a[FileFixityCouldNotRead]

    val fixityCouldNotRead = result.asInstanceOf[FileFixityCouldNotRead]

    fixityCouldNotRead.expectedFileFixity shouldBe expectedFileFixity
    fixityCouldNotRead.e shouldBe a[LocationError[_]]
    fixityCouldNotRead.e.getMessage should include(
      "The specified bucket is not valid"
    )
  }

  it("fails if the key doesn't exist in the bucket") {
    withLocalS3Bucket { bucket =>
      val checksum = randomChecksum

      val location = createObjectLocationWith(bucket)

      val expectedFileFixity = createExpectedFileFixityWith(
        location = location,
        checksum = checksum
      )

      val result =
        withFixityChecker {
          _.check(expectedFileFixity)
        }

      result shouldBe a[FileFixityCouldNotRead]

      val fixityCouldNotRead = result.asInstanceOf[FileFixityCouldNotRead]

      fixityCouldNotRead.expectedFileFixity shouldBe expectedFileFixity
      fixityCouldNotRead.e shouldBe a[LocationNotFound[_]]
      fixityCouldNotRead.e.getMessage should include(
        "Location not available!"
      )
    }
  }
}
