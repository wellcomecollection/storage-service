package uk.ac.wellcome.platform.archive.bagverifier.fixity

import org.mockito.Matchers.any
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagverifier.generators.FixityGenerators
import uk.ac.wellcome.platform.archive.common.storage.LocationNotFound
import uk.ac.wellcome.platform.archive.common.verify._
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.store.fixtures.NamespaceFixtures
import uk.ac.wellcome.storage.tags.Tags

trait FixityCheckerTestCases[Namespace, Context, StreamStoreImpl <: StreamStore[ObjectLocation]]
    extends AnyFunSpec
    with Matchers
    with EitherValues
    with NamespaceFixtures[ObjectLocation, Namespace]
    with FixityGenerators {

  def withContext[R](testWith: TestWith[Context, R]): R

  def createObjectLocationWith(namespace: Namespace): ObjectLocation

  def putString(location: ObjectLocation, contents: String)(
    implicit context: Context
  ): Unit

  def withFixityChecker[R](streamStore: StreamStoreImpl)(testWith: TestWith[FixityChecker, R])(
    implicit context: Context
  ): R

  def withStreamStoreImpl[R](testWith: TestWith[StreamStoreImpl, R])(
    implicit context: Context
  ): R

  def withTags[R](testWith: TestWith[Tags[ObjectLocation], R])(
    implicit context: Context
  ): R

  def withFixityChecker[R](testWith: TestWith[FixityChecker, R])(
    implicit context: Context
  ): R =
    withStreamStoreImpl { streamStore =>
      withFixityChecker(streamStore) { fixityChecker =>
        testWith(fixityChecker)
      }
    }

  it("returns a success if the checksum is correct") {
    withContext { implicit context =>
      withNamespace { implicit namespace =>
        val (contentString, checksum) = createStringChecksumPair

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
        val (contentString, checksum) = createStringChecksumPair

        val location = createObjectLocationWith(namespace)

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

  describe("working with tags") {
    it("sets a tag upon successful verification") {
      withContext { implicit context =>
        withNamespace { implicit namespace =>
          val location = createObjectLocationWith(namespace)

          val (contents, checksum) = createStringChecksumPair
          putString(location, contents = contents)

          val expectedFileFixity = createExpectedFileFixityWith(
            location = location,
            checksum = checksum
          )

          withFixityChecker {
            _.check(expectedFileFixity)
          } shouldBe a[FileFixityCorrect]

          val storedTags = withTags { _.get(location) }.right.value
          storedTags shouldBe Map(s"Content-${checksum.algorithm.pathRepr.toUpperCase}" -> checksum.value.toString)
        }
      }
    }

    it("skips checking if the checksum tag has been cached from a previous run") {
      withContext { implicit context =>
        withNamespace { implicit namespace =>
          val location = createObjectLocationWith(namespace)

          val (contents, checksum) = createStringChecksumPair
          putString(location, contents = contents)

          val expectedFileFixity = createExpectedFileFixityWith(
            location = location,
            checksum = checksum
          )

          withStreamStoreImpl { streamStore =>
            val spyStore = Mockito.spy(streamStore)

            val result1 = withFixityChecker(streamStore = spyStore) {
              _.check(expectedFileFixity)
            }

            verify(spyStore, times(1)).get(any[ObjectLocation])

            val result2 = withFixityChecker(streamStore = spyStore) {
              _.check(expectedFileFixity)
            }

            verify(spyStore, times(1)).get(any[ObjectLocation])

            result1 shouldBe a[FileFixityCorrect]
            result1 shouldBe result2
          }
        }
      }
    }

    it("errors if the checksum tag doesn't match") {
      withContext { implicit context =>
        withNamespace { implicit namespace =>
          val location = createObjectLocationWith(namespace)

          val (contents, checksum) = createStringChecksumPair
          putString(location, contents = contents)

          val expectedFileFixity = createExpectedFileFixityWith(
            location = location,
            checksum = checksum
          )

          // First we verify the fixity with the correct checksum, which
          // will write a Content-Type tag to the tag store.
          withFixityChecker {
            _.check(expectedFileFixity)
          }

          val storedTags1 = withTags { _.get(location) }.right.value

          val badExpectedFileFixity = expectedFileFixity.copy(
            checksum = checksum.copy(
              value = randomChecksumValue
            )
          )

          val result = withFixityChecker {
            _.check(badExpectedFileFixity)
          }

          result shouldBe a[FileFixityMismatch]

          // Check the tags weren't overridden with the bad value.
          val storedTags2 = withTags { _.get(location) }.right.value
          storedTags1 shouldBe storedTags2
        }
      }
    }

    it("errors if the checksum tag matches but the size is wrong") {
      withContext { implicit context =>
        withNamespace { implicit namespace =>
          val location = createObjectLocationWith(namespace)

          val (contents, checksum) = createStringChecksumPair
          putString(location, contents = contents)

          val expectedFileFixity = createExpectedFileFixityWith(
            location = location,
            checksum = checksum
          )

          // First we verify the fixity with the correct checksum, which
          // will write a Content-Type tag to the tag store.
          withFixityChecker {
            _.check(expectedFileFixity)
          }

          val badExpectedFileFixity = expectedFileFixity.copy(
            length = Some(contents.length + 1)
          )

          val result = withFixityChecker {
            _.check(badExpectedFileFixity)
          }

          result shouldBe a[FileFixityMismatch]
        }
      }
    }

    it("doesn't set a tag if the verification fails") {
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

          val storedTags = withTags { _.get(location) }.right.value
          storedTags shouldBe empty
        }
      }
    }

    it("adds one tag per checksum algorithm") {
      withContext { implicit context =>
        withNamespace { implicit namespace =>
          val content = "HelloWorld"
          val helloWorldChecksums = Seq(
            Checksum(MD5, ChecksumValue("68e109f0f40ca72a15e05cc22786f8e6")),
            Checksum(SHA1, ChecksumValue("db8ac1c259eb89d4a131b253bacfca5f319d54f2")),
            Checksum(SHA256, ChecksumValue("872e4e50ce9990d8b041330c47c9ddd11bec6b503ae9386a99da8584e9bb12c4")),
          )

          val location = createObjectLocationWith(namespace)
          putString(location, content)

          helloWorldChecksums.foreach { checksum =>
            val expectedFileFixity = createExpectedFileFixityWith(
              location = location,
              checksum = checksum
            )

            withFixityChecker { _.check(expectedFileFixity) } shouldBe a[FileFixityCorrect]
          }

          val storedTags = withTags { _.get(location) }.right.value
          storedTags shouldBe Map(
            "Content-MD5" -> "68e109f0f40ca72a15e05cc22786f8e6",
            "Content-SHA1" -> "db8ac1c259eb89d4a131b253bacfca5f319d54f2",
            "Content-SHA256" -> "872e4e50ce9990d8b041330c47c9ddd11bec6b503ae9386a99da8584e9bb12c4",
          )
        }
      }
    }
  }
}
