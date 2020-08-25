package uk.ac.wellcome.platform.archive.bagverifier.fixity.s3

import java.net.URI

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagverifier.fixity.{
  FileFixityCouldNotRead,
  FixityChecker,
  FixityCheckerTagsTestCases
}
import uk.ac.wellcome.platform.archive.bagverifier.storage.s3.S3Resolvable
import uk.ac.wellcome.platform.archive.bagverifier.storage.{
  LocationError,
  LocationNotFound
}
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.store.s3.S3StreamStore

class S3FixityCheckerTest
    extends FixityCheckerTagsTestCases[
      S3ObjectLocation,
      S3ObjectLocationPrefix,
      Bucket,
      Unit,
      S3StreamStore
    ]
    with S3Fixtures {
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
    testWith: TestWith[S3StreamStore, R]
  )(implicit context: Unit): R =
    testWith(
      new S3StreamStore()
    )

  override def withFixityChecker[R](
    s3Store: S3StreamStore
  )(
    testWith: TestWith[
      FixityChecker[S3ObjectLocation, S3ObjectLocationPrefix],
      R
    ]
  )(implicit context: Unit): R =
    testWith(new S3FixityChecker() {
      override val streamReader: StreamStore[S3ObjectLocation] = s3Store
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

    val expectedFileFixity = createDataDirectoryFileFixityWith(
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

    val expectedFileFixity = createDataDirectoryFileFixityWith(
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

      val expectedFileFixity = createDataDirectoryFileFixityWith(
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
