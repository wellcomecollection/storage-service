package uk.ac.wellcome.platform.archive.bagverifier.fixity

import org.mockito.Mockito
import org.mockito.Mockito.{times, verify}
import uk.ac.wellcome.platform.archive.common.verify._
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.{Identified, Location, Prefix}

trait FixityCheckerTagsTestCases[BagLocation <: Location,
  BagPrefix <: Prefix[BagLocation],
  Namespace,
  Context,
  StreamStoreImpl <: StreamStore[BagLocation]] extends FixityCheckerTestCases[BagLocation, BagPrefix, Namespace,Context, StreamStoreImpl]{

    val contentString = "HelloWorld"
    val checksumString = "68e109f0f40ca72a15e05cc22786f8e6"
    val checksum = Checksum(MD5, ChecksumValue(checksumString))

    it("sets a tag on a successfully-verified object") {
      withContext { implicit context =>
        withNamespace { implicit namespace =>
          val location = createLocationWith(namespace)
          putString(location, contentString)

          val expectedFileFixity = createDataDirectoryFileFixityWith(
            location = location,
            checksum = checksum
          )

          withFixityChecker { fixityChecker =>
            fixityChecker.check(expectedFileFixity) shouldBe a[
              FileFixityCorrect[_]
            ]
            fixityChecker.tags shouldBe defined
            fixityChecker.tags.get.get(location).right.value shouldBe Identified(
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

          val expectedFileFixity = createDataDirectoryFileFixityWith(
            location = location,
            checksum = checksum
          )

          withStreamStore { streamStore =>
            val spyStore: StreamStoreImpl = Mockito.spy(streamStore)

            withFixityChecker(spyStore) { fixityChecker =>
              fixityChecker.check(expectedFileFixity) shouldBe a[
                FileFixityCorrect[_]
              ]

              // StreamStore.get() should have been called to read the object so
              // it can be verified.
              verify(spyStore, times(1)).get(location)

              // It shouldn't be read a second time, because we see the tag written by
              // the previous verification.
              fixityChecker.check(expectedFileFixity) shouldBe a[
                FileFixityCorrect[_]
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

          val expectedFileFixity = createDataDirectoryFileFixityWith(
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
                FileFixityCorrect[_]
              ]

              // StreamStore.get() should have been called to read the object so
              // it can be verified.
              verify(spyStore, times(1)).get(location)

              // It shouldn't be read a second time, because we see the tag written by
              // the previous verification.
              val result = fixityChecker.check(badExpectedFixity)
              result shouldBe a[FileFixityMismatch[_]]
              result
                .asInstanceOf[FileFixityMismatch[BagLocation]]
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

          val expectedFileFixity = createFetchFileFixityWith(
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
                FileFixityCorrect[_]
              ]

              // StreamStore.get() should have been called to read the object so
              // it can be verified.
              verify(spyStore, times(1)).get(location)

              // It shouldn't be read a second time, because we see the tag written by
              // the previous verification.
              val result = fixityChecker.check(badExpectedFixity)
              result shouldBe a[FileFixityMismatch[_]]
              result
                .asInstanceOf[FileFixityMismatch[BagLocation]]
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

          val expectedFileFixity = createDataDirectoryFileFixityWith(
            location = location
          )

          withFixityChecker { fixityChecker =>
            fixityChecker.check(expectedFileFixity) shouldBe a[
              FileFixityMismatch[_]
            ]

            fixityChecker.tags shouldBe defined
            fixityChecker.tags.get.get(location).right.value shouldBe Identified(
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
              val expectedFileFixity = createDataDirectoryFileFixityWith(
                location = location,
                checksum = checksum
              )

              fixityChecker.check(expectedFileFixity) shouldBe a[
                FileFixityCorrect[_]
              ]
            }

            fixityChecker.tags shouldBe defined
            fixityChecker.tags.get.get(location).right.value shouldBe Identified(
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
