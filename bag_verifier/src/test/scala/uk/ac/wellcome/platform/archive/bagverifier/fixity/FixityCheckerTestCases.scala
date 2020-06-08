package uk.ac.wellcome.platform.archive.bagverifier.fixity

import java.net.URI

import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.bagit.models.BagPath
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.platform.archive.common.storage.LocationNotFound
import uk.ac.wellcome.platform.archive.common.verify._
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators
import uk.ac.wellcome.storage.store.fixtures.NamespaceFixtures
import uk.ac.wellcome.storage.tags.Tags

trait FixityCheckerTestCases[Namespace, Context]
    extends AnyFunSpec
    with Matchers
    with EitherValues
    with NamespaceFixtures[ObjectLocation, Namespace]
    with StorageRandomThings
    with ObjectLocationGenerators {

  def randomChecksum = Checksum(SHA256, randomChecksumValue)
  def badChecksum = Checksum(MD5, randomChecksumValue)

  def createExpectedFileFixity: ExpectedFileFixity =
    createExpectedFileFixityWith()

  def resolve(location: ObjectLocation): URI

  def createExpectedFileFixityWith(
    location: ObjectLocation = createObjectLocation,
    checksum: Checksum = randomChecksum,
    length: Option[Long] = None
  ): ExpectedFileFixity = {
    ExpectedFileFixity(
      uri = resolve(location),
      path = BagPath(randomAlphanumeric),
      checksum = checksum,
      length = length
    )
  }

  def withContext[R](testWith: TestWith[Context, R]): R

  def createObjectLocationWith(namespace: Namespace): ObjectLocation

  def putString(location: ObjectLocation, contents: String)(
    implicit context: Context
  ): Unit

  def withFixityChecker[R](testWith: TestWith[FixityChecker, R])(
    implicit context: Context
  ): R

  def withTags[R](testWith: TestWith[Tags[ObjectLocation], R])(
    implicit context: Context
  ): R

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

        val expectedFileFixity = createExpectedFileFixityWith(
          location = location,
          checksum = checksum
        )

        val result =
          withFixityChecker {
            _.check(expectedFileFixity)
          }

        result shouldBe a[FileFixityCorrect]

        val fixityCorrect = result.asInstanceOf[FileFixityCorrect]
        fixityCorrect.expectedFileFixity shouldBe expectedFileFixity
        fixityCorrect.size shouldBe contentString.getBytes.length
      }
    }
  }

  it("fails if the object doesn't exist") {
    withContext { implicit context =>
      withNamespace { implicit namespace =>
        val checksum = randomChecksum

        val location = createObjectLocationWith(namespace)

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

  it("fails if the checksum is incorrect") {
    withContext { implicit context =>
      withNamespace { implicit namespace =>
        val checksum = randomChecksum

        val location = createObjectLocationWith(namespace)
        putString(location, randomAlphanumeric)

        val expectedFileFixity = createExpectedFileFixityWith(
          location = location,
          checksum = checksum
        )

        val result =
          withFixityChecker {
            _.check(expectedFileFixity)
          }

        result shouldBe a[FileFixityMismatch]

        val fixityMismatch = result.asInstanceOf[FileFixityMismatch]

        fixityMismatch.expectedFileFixity shouldBe expectedFileFixity
        fixityMismatch.e shouldBe a[FailedChecksumNoMatch]
        fixityMismatch.e.getMessage should startWith(
          s"Checksum values do not match! Expected: $checksum"
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

        val expectedFileFixity = createExpectedFileFixityWith(
          location = location,
          checksum = checksum,
          length = Some(contentString.getBytes().length - 1)
        )

        putString(location, contentString)

        val result =
          withFixityChecker {
            _.check(expectedFileFixity)
          }

        result shouldBe a[FileFixityMismatch]

        val fixityMismatch = result.asInstanceOf[FileFixityMismatch]

        fixityMismatch.expectedFileFixity shouldBe expectedFileFixity
        fixityMismatch.e shouldBe a[Throwable]
        fixityMismatch.e.getMessage should startWith(
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

        val expectedFileFixity = createExpectedFileFixityWith(
          location = location,
          checksum = checksum,
          length = Some(contentString.getBytes().length)
        )

        putString(location, contentString)

        val result =
          withFixityChecker {
            _.check(expectedFileFixity)
          }

        result shouldBe a[FileFixityCorrect]

        val fixityCorrect = result.asInstanceOf[FileFixityCorrect]
        fixityCorrect.expectedFileFixity shouldBe expectedFileFixity
        fixityCorrect.size shouldBe contentString.getBytes.length
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

        val expectedFileFixity = createExpectedFileFixityWith(
          location = location,
          checksum = checksum
        )

        putString(location, contentString)

        val result =
          withFixityChecker {
            _.check(expectedFileFixity)
          }

        result shouldBe a[FileFixityCorrect]

        val fixityCorrect = result.asInstanceOf[FileFixityCorrect]
        fixityCorrect.expectedFileFixity shouldBe expectedFileFixity
        fixityCorrect.size shouldBe contentString.getBytes.length
      }
    }
  }

  describe("working with tags") {
    it("sets a tag upon successful verification") {
      withContext { implicit context =>
        withNamespace { implicit namespace =>
          val location = createObjectLocationWith(namespace)

          // md5("HelloWorld")
          val checksum = Checksum(
            algorithm = MD5,
            value = ChecksumValue("68e109f0f40ca72a15e05cc22786f8e6")
          )
          putString(location, contents = "HelloWorld")

          val expectedFileFixity = createExpectedFileFixityWith(
            location = location,
            checksum = checksum
          )

          withFixityChecker {
            _.check(expectedFileFixity)
          } shouldBe a[FileFixityCorrect]

          val storedTags = withTags { _.get(location) }.right.value
          storedTags shouldBe Map("Content-MD5" -> "68e109f0f40ca72a15e05cc22786f8e6")
        }
      }
    }

    it("skips checking if the checksum tag and size are correct") {
      true shouldBe false
    }

    it("errors if the checksum tag doesn't match") {
      true shouldBe false
    }

    it("errors if the checksum tag matches but the size is wrong") {
      true shouldBe false
    }

    it("doesn't set a tag upon if the verification fails") {
      true shouldBe false
    }

    it("adds one tag per checksum algorithm") {
      true shouldBe false
    }
  }
}
