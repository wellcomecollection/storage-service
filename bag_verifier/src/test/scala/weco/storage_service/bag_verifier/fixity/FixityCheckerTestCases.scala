package weco.storage_service.bag_verifier.fixity

import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.fixtures.TestWith
import weco.storage_service.bag_verifier.generators.FixityGenerators
import weco.storage_service.bag_verifier.storage.LocationNotFound
import weco.storage_service.checksum._
import weco.storage.store.Readable
import weco.storage.store.fixtures.NamespaceFixtures
import weco.storage.streaming.InputStreamWithLength
import weco.storage.{Location, Prefix}

trait FixityCheckerTestCases[
  BagLocation <: Location,
  BagPrefix <: Prefix[BagLocation],
  Namespace,
  Context,
  StreamReaderImpl <: Readable[BagLocation, InputStreamWithLength]
] extends AnyFunSpec
    with Matchers
    with EitherValues
    with NamespaceFixtures[BagLocation, Namespace]
    with FixityGenerators[BagLocation] {

  def withContext[R](testWith: TestWith[Context, R]): R

  def createLocationWith(namespace: Namespace): BagLocation =
    createId(namespace)

  override def createLocation: BagLocation =
    withNamespace { namespace =>
      createLocationWith(namespace)
    }

  def putString(location: BagLocation, contents: String)(
    implicit context: Context
  ): Unit

  def withFixityChecker[R](streamReader: StreamReaderImpl)(
    testWith: TestWith[FixityChecker[BagLocation, BagPrefix], R]
  )(
    implicit context: Context
  ): R

  def withStreamReader[R](testWith: TestWith[StreamReaderImpl, R])(
    implicit context: Context
  ): R

  def withFixityChecker[R]()(
    testWith: TestWith[FixityChecker[BagLocation, BagPrefix], R]
  )(
    implicit context: Context
  ): R =
    withStreamReader { streamReader =>
      withFixityChecker(streamReader) { fixityChecker =>
        testWith(fixityChecker)
      }
    }

  val contentString = "HelloWorld"

  val multiChecksum = MultiManifestChecksum(
    md5 = Some(ChecksumValue("68e109f0f40ca72a15e05cc22786f8e6")),
    sha1 = Some(ChecksumValue("db8ac1c259eb89d4a131b253bacfca5f319d54f2")),
    sha256 = Some(ChecksumValue("872e4e50ce9990d8b041330c47c9ddd11bec6b503ae9386a99da8584e9bb12c4")),
    sha512 = Some(ChecksumValue("8ae6ae71a75d3fb2e0225deeb004faf95d816a0a58093eb4cb5a3aa0f197050d7a4dc0a2d5c6fbae5fb5b0d536a0a9e6b686369fa57a027687c3630321547596"))
  )

  it("returns a success if the checksum is correct") {
    withContext { implicit context =>
      withNamespace { implicit namespace =>
        val location = createLocationWith(namespace)
        putString(location, contentString)

        val expectedFileFixity = createDataDirectoryFileFixityWith(
          location = location,
          multiChecksum = multiChecksum
        )

        val result =
          withFixityChecker() {
            _.check(expectedFileFixity)
          }

        result shouldBe a[FileFixityCorrect[_]]

        val fixityCorrect = result.asInstanceOf[FileFixityCorrect[BagLocation]]
        fixityCorrect.expectedFileFixity shouldBe expectedFileFixity
        fixityCorrect.size shouldBe contentString.getBytes.length
      }
    }
  }

  it("fails if the object doesn't exist") {
    withContext { implicit context =>
      withNamespace { implicit namespace =>
        val location = createLocationWith(namespace)

        val expectedFileFixity = createDataDirectoryFileFixityWith(
          location = location,
          multiChecksum = randomMultiChecksum
        )

        val result =
          withFixityChecker() {
            _.check(expectedFileFixity)
          }

        result shouldBe a[FileFixityCouldNotRead[_]]

        val fixityCouldNotRead =
          result.asInstanceOf[FileFixityCouldNotRead[BagLocation]]

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
        val multiChecksum = randomMultiChecksum

        val location = createLocationWith(namespace)
        putString(location, randomAlphanumeric())

        val expectedFileFixity = createDataDirectoryFileFixityWith(
          location = location,
          multiChecksum = multiChecksum
        )

        val result =
          withFixityChecker() {
            _.check(expectedFileFixity)
          }

        result shouldBe a[FileFixityMismatch[_]]

        val fixityMismatch =
          result.asInstanceOf[FileFixityMismatch[BagLocation]]

        fixityMismatch.expectedFileFixity shouldBe expectedFileFixity
        fixityMismatch.e shouldBe a[FailedChecksumNoMatch]
        fixityMismatch.e.getMessage should startWith(
          s"Checksum values do not match! Expected: $multiChecksum"
        )
      }
    }
  }

  it("fails if the checksum is correct but the expected length is wrong") {
    withContext { implicit context =>
      withNamespace { implicit namespace =>
        val location = createLocationWith(namespace)

        val expectedFileFixity = createFetchFileFixityWith(
          location = location,
          multiChecksum = multiChecksum,
          length = Some(contentString.getBytes().length - 1)
        )

        putString(location, contentString)

        val result =
          withFixityChecker() {
            _.check(expectedFileFixity)
          }

        result shouldBe a[FileFixityMismatch[_]]

        val fixityMismatch =
          result.asInstanceOf[FileFixityMismatch[BagLocation]]

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
        val location = createLocationWith(namespace)

        val expectedFileFixity = createDataDirectoryFileFixityWith(
          location = location,
          multiChecksum = multiChecksum
        )

        putString(location, contentString)

        val result =
          withFixityChecker() {
            _.check(expectedFileFixity)
          }

        result shouldBe a[FileFixityCorrect[_]]

        val fixityCorrect = result.asInstanceOf[FileFixityCorrect[BagLocation]]
        fixityCorrect.expectedFileFixity shouldBe expectedFileFixity
        fixityCorrect.size shouldBe contentString.getBytes.length
      }
    }
  }

  it("supports different checksum algorithms") {
    withContext { implicit context =>
      withNamespace { implicit namespace =>
        val location = createLocationWith(namespace)

        val expectedFileFixity = createDataDirectoryFileFixityWith(
          location = location,
          multiChecksum = multiChecksum
        )

        putString(location, contentString)

        val result =
          withFixityChecker() {
            _.check(expectedFileFixity)
          }

        result shouldBe a[FileFixityCorrect[_]]

        val fixityCorrect = result.asInstanceOf[FileFixityCorrect[BagLocation]]
        fixityCorrect.expectedFileFixity shouldBe expectedFileFixity
        fixityCorrect.size shouldBe contentString.getBytes.length
      }
    }
  }
}
