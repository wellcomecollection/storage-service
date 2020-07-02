package uk.ac.wellcome.platform.archive.bagverifier.fixity.s3

import java.net.URI

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagverifier.fixity.{
  FileFixityCouldNotRead,
  FixityChecker,
  FixityCheckerTestCases
}
import uk.ac.wellcome.platform.archive.bagverifier.storage.s3.S3Resolvable
import uk.ac.wellcome.platform.archive.bagverifier.storage.{
  LocationError,
  LocationNotFound
}
import uk.ac.wellcome.storage.S3ObjectLocation
import uk.ac.wellcome.storage.fixtures.NewS3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.store.s3.NewS3StreamStore

class S3FixityCheckerTest
    extends FixityCheckerTestCases[
      S3ObjectLocation,
      Bucket,
      Unit,
      NewS3StreamStore
    ]
    with NewS3Fixtures {
  override def withContext[R](testWith: TestWith[Unit, R]): R =
    testWith(())

  override def putString(location: S3ObjectLocation, contents: String)(
    implicit context: Unit
  ): Unit =
    s3Client.putObject(
      location.bucket,
      location.key,
      contents
    )

  override def withStreamStore[R](
    testWith: TestWith[NewS3StreamStore, R]
  )(implicit context: Unit): R =
    testWith(
      new NewS3StreamStore()
    )

  override def withFixityChecker[R](
    s3Store: NewS3StreamStore
  )(
    testWith: TestWith[FixityChecker[S3ObjectLocation], R]
  )(implicit context: Unit): R =
    testWith(new S3FixityChecker() {
      override val streamStore: StreamStore[S3ObjectLocation] = s3Store
    })

  implicit val context: Unit = ()

  implicit val s3Resolvable: S3Resolvable = new S3Resolvable()

  override def resolve(location: S3ObjectLocation): URI =
    s3Resolvable.resolve(location)

  override def withNamespace[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  override def createId(implicit bucket: Bucket): S3ObjectLocation =
    createS3ObjectLocationWith(bucket)

  it("fails if the bucket doesn't exist") {
    val location = createS3ObjectLocationWith(bucket = createBucket)

    val expectedFileFixity = createExpectedFileFixityWith(
      location = location
    )

    val result =
      withFixityChecker {
        _.check(expectedFileFixity)
      }

    result shouldBe a[FileFixityCouldNotRead[_]]

    val fixityCouldNotRead =
      result.asInstanceOf[FileFixityCouldNotRead[S3ObjectLocation]]

    fixityCouldNotRead.expectedFileFixity shouldBe expectedFileFixity
    fixityCouldNotRead.e shouldBe a[LocationNotFound[_]]
    fixityCouldNotRead.e.getMessage should include(
      "Location not available!"
    )
  }

  it("fails if the bucket name is invalid") {
    val location = createS3ObjectLocationWith(bucket = createInvalidBucket)

    val expectedFileFixity = createExpectedFileFixityWith(
      location = location
    )

    val result =
      withFixityChecker {
        _.check(expectedFileFixity)
      }

    result shouldBe a[FileFixityCouldNotRead[_]]

    val fixityCouldNotRead =
      result.asInstanceOf[FileFixityCouldNotRead[S3ObjectLocation]]

    fixityCouldNotRead.expectedFileFixity shouldBe expectedFileFixity
    fixityCouldNotRead.e shouldBe a[LocationError[_]]
    fixityCouldNotRead.e.getMessage should include(
      "The specified bucket is not valid"
    )
  }

  it("fails if the key doesn't exist in the bucket") {
    withLocalS3Bucket { bucket =>
      val location = createS3ObjectLocationWith(bucket)

      val expectedFileFixity = createExpectedFileFixityWith(
        location = location
      )

      val result =
        withFixityChecker {
          _.check(expectedFileFixity)
        }

      result shouldBe a[FileFixityCouldNotRead[_]]

      val fixityCouldNotRead =
        result.asInstanceOf[FileFixityCouldNotRead[S3ObjectLocation]]

      fixityCouldNotRead.expectedFileFixity shouldBe expectedFileFixity
      fixityCouldNotRead.e shouldBe a[LocationNotFound[_]]
      fixityCouldNotRead.e.getMessage should include(
        "Location not available!"
      )
    }
  }
}
