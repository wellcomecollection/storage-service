package weco.storage_service.bag_verifier.fixity.s3

import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.PutObjectRequest

import java.net.URI
import weco.fixtures.TestWith
import weco.storage_service.bag_verifier.fixity.{FileFixityCouldNotRead, FixityChecker, FixityCheckerTagsTestCases}
import weco.storage_service.bag_verifier.storage.s3.{S3Locatable, S3Resolvable}
import weco.storage_service.bag_verifier.storage.{LocationError, LocationNotFound}
import weco.storage.fixtures.S3Fixtures
import weco.storage.fixtures.S3Fixtures.Bucket
import weco.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import weco.storage.services.s3.S3SizeFinder
import weco.storage.store.s3.S3StreamStore
import weco.storage.tags.s3.S3Tags

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
  ): Unit = {
    val putRequest =
      PutObjectRequest.builder()
        .bucket(location.bucket)
        .key(location.key)
        .build()

    val requestBody = RequestBody.fromString(contents)

    s3ClientV2.putObject(putRequest, requestBody)
  }

  override def withStreamReader[R](
    testWith: TestWith[S3StreamStore, R]
  )(implicit context: Unit): R =
    testWith(
      new S3StreamStore()
    )

  override def withFixityChecker[R](
    s3Reader: S3StreamStore
  )(
    testWith: TestWith[
      FixityChecker[S3ObjectLocation, S3ObjectLocationPrefix],
      R
    ]
  )(implicit context: Unit): R =
    testWith(
      new S3FixityChecker(
        s3Reader,
        new S3SizeFinder(),
        new S3Tags(),
        S3Locatable.s3UriLocatable
      )
    )

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
      withFixityChecker() {
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
      withFixityChecker() {
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
        withFixityChecker() {
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
