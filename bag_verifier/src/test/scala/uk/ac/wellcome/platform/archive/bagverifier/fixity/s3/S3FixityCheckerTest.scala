package uk.ac.wellcome.platform.archive.bagverifier.fixity.s3

import java.net.URI

import org.mockito.Mockito
import org.mockito.Mockito.{times, verify}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagverifier.fixity.{FileFixityCorrect, FileFixityCouldNotRead, FileFixityMismatch, FixityChecker, FixityCheckerTestCases}
import uk.ac.wellcome.platform.archive.bagverifier.storage.s3.S3Resolvable
import uk.ac.wellcome.platform.archive.bagverifier.storage.{LocationError, LocationNotFound}
import uk.ac.wellcome.platform.archive.common.verify.{Checksum, ChecksumValue, MD5, SHA1, SHA256}
import uk.ac.wellcome.storage.Identified
import uk.ac.wellcome.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.store.s3.S3StreamStore

class S3FixityCheckerTest
    extends FixityCheckerTestCases[
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


  describe("handles tags") {
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

            fixityChecker.tags.foreach{t =>t.get(location).right.value shouldBe Identified(
              location,
              Map(
                "Content-MD5" -> checksumString
              )
            )}
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
            val spyStore = Mockito.spy(streamStore)

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
            val spyStore = Mockito.spy(streamStore)

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
                .asInstanceOf[FileFixityMismatch[S3ObjectLocation]]
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
            val spyStore = Mockito.spy(streamStore)

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
                .asInstanceOf[FileFixityMismatch[S3ObjectLocation]]
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

            fixityChecker.tags.foreach{t => t.get(location).right.value shouldBe Identified(
              location,
              Map.empty
            )}
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

            fixityChecker.tags.foreach {t =>
              t.get(location).right.value shouldBe Identified(
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
}
