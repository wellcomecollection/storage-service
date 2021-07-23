package weco.storage_service.bag_verifier.fixity

import org.mockito.Mockito
import org.mockito.Mockito.{times, verify}
import weco.storage_service.checksum._
import weco.storage.store.Readable
import weco.storage.streaming.InputStreamWithLength
import weco.storage.{Identified, Location, Prefix}

trait FixityCheckerTagsTestCases[BagLocation <: Location, BagPrefix <: Prefix[
  BagLocation
], Namespace, Context, StreamReaderImpl <: Readable[
  BagLocation,
  InputStreamWithLength
]] extends FixityCheckerTestCases[
      BagLocation,
      BagPrefix,
      Namespace,
      Context,
      StreamReaderImpl
    ] {

  def tagName(algorithm: ChecksumAlgorithm): String =
    algorithm match {
      case MD5    => "Content-MD5"
      case SHA1   => "Content-SHA1"
      case SHA256 => "Content-SHA256"
      case SHA512 => "Content-SHA512"
    }

  it("sets a tag on a successfully-verified object") {
    withContext { implicit context =>
      withNamespace { implicit namespace =>
        val location = createLocationWith(namespace)
        putString(location, contentString)

        val multiChecksum = MultiManifestChecksum(
          md5 = None,
          sha1 = None,
          sha256 = Some(ChecksumValue("872e4e50ce9990d8b041330c47c9ddd11bec6b503ae9386a99da8584e9bb12c4")),
          sha512 = None
        )

        val expectedFileFixity = createDataDirectoryFileFixityWith(
          location = location,
          multiChecksum = multiChecksum
        )

        withFixityChecker() { fixityChecker =>
          fixityChecker.check(expectedFileFixity) shouldBe a[
            FileFixityCorrect[_]
          ]
          fixityChecker.tags.get(location).value shouldBe Identified(
            location,
            Map(
              tagName(SHA256) -> "872e4e50ce9990d8b041330c47c9ddd11bec6b503ae9386a99da8584e9bb12c4"
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
          multiChecksum = multiChecksum
        )

        withStreamReader { streamReader =>
          val spyReader: StreamReaderImpl = Mockito.spy(streamReader)

          withFixityChecker(spyReader) { fixityChecker =>
            fixityChecker.check(expectedFileFixity) shouldBe a[
              FileFixityCorrect[_]
            ]

            // Readable.get() should have been called to read the object so
            // it can be verified.
            verify(spyReader, times(1)).get(location)

            // It shouldn't be read a second time, because we see the tag written by
            // the previous verification.
            fixityChecker.check(expectedFileFixity) shouldBe a[
              FileFixityCorrect[_]
            ]
            verify(spyReader, times(1)).get(location)
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
          multiChecksum = multiChecksum
        )

        val badExpectedFixity = expectedFileFixity.copy(
          multiChecksum = multiChecksum.copy(
            sha256 = Some(randomChecksumValue)
          )
        )

        withStreamReader { streamReader =>
          val spyReader: StreamReaderImpl = Mockito.spy(streamReader)

          withFixityChecker(spyReader) { fixityChecker =>
            fixityChecker.check(expectedFileFixity) shouldBe a[
              FileFixityCorrect[_]
            ]

            // Readable.get() should have been called to read the object so
            // it can be verified.
            verify(spyReader, times(1)).get(location)

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
            verify(spyReader, times(1)).get(location)
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
          multiChecksum = multiChecksum
        )

        val badExpectedFixity = expectedFileFixity.copy(
          length = Some(contentString.length + 1)
        )

        withStreamReader { streamReader =>
          val spyReadable: StreamReaderImpl = Mockito.spy(streamReader)

          withFixityChecker(spyReadable) { fixityChecker =>
            fixityChecker.check(expectedFileFixity) shouldBe a[
              FileFixityCorrect[_]
            ]

            // Readable.get() should have been called to read the object so
            // it can be verified.
            verify(spyReadable, times(1)).get(location)

            // It shouldn't be read a second time, because we see the tag written by
            // the previous verification.
            val result = fixityChecker.check(badExpectedFixity)
            result shouldBe a[FileFixityMismatch[_]]
            result
              .asInstanceOf[FileFixityMismatch[BagLocation]]
              .e
              .getMessage should startWith("Lengths do not match")
            verify(spyReadable, times(1)).get(location)
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

        withFixityChecker() { fixityChecker =>
          fixityChecker.check(expectedFileFixity) shouldBe a[
            FileFixityMismatch[_]
          ]

          fixityChecker.tags.get(location).value shouldBe Identified(
            location,
            Map.empty
          )
        }
      }
    }
  }

  it("adds one tag per checksum algorithm") {
    withContext { implicit context =>
      withNamespace { implicit namespace =>
        val location = createLocationWith(namespace)
        putString(location, contentString)

        withFixityChecker() { fixityChecker =>
          val expectedFileFixity = createDataDirectoryFileFixityWith(
            location = location,
            multiChecksum = multiChecksum
          )

          fixityChecker.check(expectedFileFixity) shouldBe a[
            FileFixityCorrect[_]
          ]

          val expectedTags =
            multiChecksum
              .definedChecksums
              .map { case (algorithm, checksumValue) =>
                tagName(algorithm) -> checksumValue.value
              }
              .toMap

          fixityChecker.tags.get(location).value shouldBe Identified(location, expectedTags)
        }
      }
    }
  }
}
