package uk.ac.wellcome.platform.archive.bagverifier.fixity

import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagverifier.generators.FixityGenerators
import uk.ac.wellcome.platform.archive.bagverifier.storage.LocationNotFound
import uk.ac.wellcome.platform.archive.common.verify._
import uk.ac.wellcome.storage.{Identified, Location}
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.store.fixtures.NamespaceFixtures

trait FixityCheckerTestCases[
    BagLocation <: Location,
    Namespace,
    Context,
    StreamStoreImpl <: StreamStore[BagLocation]]
  extends AnyFunSpec
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

  def withFixityChecker[R](streamStore: StreamStoreImpl)(
    testWith: TestWith[FixityChecker[BagLocation], R]
  )(
    implicit context: Context
  ): R

  def withStreamStore[R](testWith: TestWith[StreamStoreImpl, R])(
    implicit context: Context
  ): R

  def withFixityChecker[R](testWith: TestWith[FixityChecker[BagLocation], R])(
    implicit context: Context
  ): R =
    withStreamStore { streamStore =>
      withFixityChecker(streamStore) { fixityChecker =>
        testWith(fixityChecker)
      }
    }

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

        val location = createLocationWith(namespace)
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

        val location = createLocationWith(namespace)

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

        val location = createLocationWith(namespace)
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

        val location = createLocationWith(namespace)
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

        val location = createLocationWith(namespace)
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

        val location = createLocationWith(namespace)
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

  describe("handles tags") {
    val contentString = "HelloWorld"
    val checksumString = "68e109f0f40ca72a15e05cc22786f8e6"
    val checksum = Checksum(MD5, ChecksumValue(checksumString))

    it("sets a tag on a successfully-verified object") {
      withContext { implicit context =>
        withNamespace { implicit namespace =>
          val location = createLocationWith(namespace)
          putString(location, contentString)

          val expectedFileFixity = createExpectedFileFixityWith(
            location = location,
            checksum = checksum
          )

          withFixityChecker { fixityChecker =>
            fixityChecker.check(expectedFileFixity) shouldBe a[
              FileFixityCorrect
            ]

            fixityChecker.tags.get(location).right.value shouldBe Identified(
              location,
              Map(
                "Content-MD5" -> checksumString
              )
            )
          }
        }
      }
    }

    it("skips checking if there's a matching tag from a previous verification") {
      withContext { implicit context =>
        withNamespace { implicit namespace =>
          val location = createLocationWith(namespace)
          putString(location, contentString)

          val expectedFileFixity = createExpectedFileFixityWith(
            location = location,
            checksum = checksum
          )

          withStreamStore { streamStore =>
            val spyStore: StreamStoreImpl = Mockito.spy(streamStore)

            withFixityChecker(spyStore) { fixityChecker =>
              fixityChecker.check(expectedFileFixity) shouldBe a[
                FileFixityCorrect
              ]

              // StreamStore.get() should have been called to read the object so
              // it can be verified.
              verify(spyStore, times(1)).get(location)

              // It shouldn't be read a second time, because we see the tag written by
              // the previous verification.
              fixityChecker.check(expectedFileFixity) shouldBe a[
                FileFixityCorrect
              ]
              verify(spyStore, times(1)).get(location)
            }
          }
        }
      }
    }

    it("errors if there's a mismatched tag from a previous verification") {
      withContext { implicit context =>
        withNamespace { implicit namespace =>
          val location = createLocationWith(namespace)
          putString(location, contentString)

          val expectedFileFixity = createExpectedFileFixityWith(
            location = location,
            checksum = checksum
          )

          val badExpectedFixity = expectedFileFixity.copy(
            checksum = checksum.copy(
              value = randomChecksumValue
            )
          )

          withStreamStore { streamStore =>
            val spyStore: StreamStoreImpl = Mockito.spy(streamStore)

            withFixityChecker(spyStore) { fixityChecker =>
              fixityChecker.check(expectedFileFixity) shouldBe a[
                FileFixityCorrect
              ]

              // StreamStore.get() should have been called to read the object so
              // it can be verified.
              verify(spyStore, times(1)).get(location)

              // It shouldn't be read a second time, because we see the tag written by
              // the previous verification.
              val result = fixityChecker.check(badExpectedFixity)
              result shouldBe a[FileFixityMismatch]
              result
                .asInstanceOf[FileFixityMismatch]
                .e
                .getMessage should startWith(
                "Cached verification tag doesn't match expected checksum"
              )
              verify(spyStore, times(1)).get(location)
            }
          }
        }
      }
    }

    it("errors if there's a matching tag but the size is wrong") {
      withContext { implicit context =>
        withNamespace { implicit namespace =>
          val location = createLocationWith(namespace)
          putString(location, contentString)

          val expectedFileFixity = createExpectedFileFixityWith(
            location = location,
            checksum = checksum
          )

          val badExpectedFixity = expectedFileFixity.copy(
            length = Some(contentString.length + 1)
          )

          withStreamStore { streamStore =>
            val spyStore: StreamStoreImpl = Mockito.spy(streamStore)

            withFixityChecker(spyStore) { fixityChecker =>
              fixityChecker.check(expectedFileFixity) shouldBe a[
                FileFixityCorrect
              ]

              // StreamStore.get() should have been called to read the object so
              // it can be verified.
              verify(spyStore, times(1)).get(location)

              // It shouldn't be read a second time, because we see the tag written by
              // the previous verification.
              val result = fixityChecker.check(badExpectedFixity)
              result shouldBe a[FileFixityMismatch]
              result
                .asInstanceOf[FileFixityMismatch]
                .e
                .getMessage should startWith("Lengths do not match")
              verify(spyStore, times(1)).get(location)
            }
          }
        }
      }
    }

    it("doesn't set a tag if the verification fails") {
      withContext { implicit context =>
        withNamespace { implicit namespace =>
          val location = createLocationWith(namespace)
          putString(location, contentString)

          val expectedFileFixity = createExpectedFileFixityWith(
            location = location
          )

          withFixityChecker { fixityChecker =>
            fixityChecker.check(expectedFileFixity) shouldBe a[
              FileFixityMismatch
            ]

            fixityChecker.tags.get(location).right.value shouldBe Identified(
              location,
              Map.empty
            )
          }
        }
      }
    }

    it("adds one tag per checksum algorithm") {
      val contentString = "HelloWorld"

      val allChecksums = Seq(
        Checksum(MD5, ChecksumValue("68e109f0f40ca72a15e05cc22786f8e6")),
        Checksum(
          SHA1,
          ChecksumValue("db8ac1c259eb89d4a131b253bacfca5f319d54f2")
        ),
        Checksum(
          SHA256,
          ChecksumValue(
            "872e4e50ce9990d8b041330c47c9ddd11bec6b503ae9386a99da8584e9bb12c4"
          )
        )
      )

      withContext { implicit context =>
        withNamespace { implicit namespace =>
          val location = createLocationWith(namespace)
          putString(location, contentString)

          withFixityChecker { fixityChecker =>
            allChecksums.foreach { checksum =>
              val expectedFileFixity = createExpectedFileFixityWith(
                location = location,
                checksum = checksum
              )

              fixityChecker.check(expectedFileFixity) shouldBe a[
                FileFixityCorrect
              ]
            }

            fixityChecker.tags.get(location).right.value shouldBe Identified(
              location,
              Map(
                "Content-MD5" -> "68e109f0f40ca72a15e05cc22786f8e6",
                "Content-SHA1" -> "db8ac1c259eb89d4a131b253bacfca5f319d54f2",
                "Content-SHA256" -> "872e4e50ce9990d8b041330c47c9ddd11bec6b503ae9386a99da8584e9bb12c4"
              )
            )
          }
        }
      }
    }
  }
}
