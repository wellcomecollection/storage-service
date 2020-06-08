package uk.ac.wellcome.platform.archive.bagverifier.fixity.s3

import java.net.URI

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagverifier.fixity.{FileFixityCouldNotRead, FixityChecker, FixityCheckerTestCases}
import uk.ac.wellcome.platform.archive.common.storage.services.S3Resolvable
import uk.ac.wellcome.platform.archive.common.storage.{LocationError, LocationNotFound}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.store.fixtures.BucketNamespaceFixtures
import uk.ac.wellcome.storage.store.s3.S3StreamStore
import uk.ac.wellcome.storage.tags.Tags
import uk.ac.wellcome.storage.tags.s3.S3Tags

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

  override def withTags[R](testWith: TestWith[Tags[ObjectLocation], R])(
    implicit context: Unit
  ): R =
    testWith(new S3Tags())

  implicit val context: Unit = ()

  implicit val s3Resolvable: S3Resolvable = new S3Resolvable()

  import uk.ac.wellcome.platform.archive.common.storage.Resolvable._

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

  override def withFixityChecker[R](customStreamStore: S3StreamStore)(testWith: TestWith[FixityChecker, R])(implicit context: Unit): R =
    testWith(
      new S3FixityChecker() {
        override val streamStore: S3StreamStore = customStreamStore
      }
    )

  override def withStreamStoreImpl[R](testWith: TestWith[S3StreamStore, R])(implicit context:  Unit): R =
    testWith(new S3StreamStore())
}
