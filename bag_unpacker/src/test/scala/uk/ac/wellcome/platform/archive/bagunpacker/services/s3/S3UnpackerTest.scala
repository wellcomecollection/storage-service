package uk.ac.wellcome.platform.archive.bagunpacker.services.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.AmazonS3Exception
import org.scalatest.Assertion
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagunpacker.models.UnpackSummary
import uk.ac.wellcome.platform.archive.bagunpacker.services.{
  Unpacker,
  UnpackerTestCases
}
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestFailed,
  IngestStepResult
}
import uk.ac.wellcome.storage.{Identified, ObjectLocation}
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.s3.S3ClientFactory
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.store.s3.S3StreamStore
import uk.ac.wellcome.storage.streaming.{
  InputStreamWithLength,
  InputStreamWithLengthAndMetadata
}

import scala.util.Try

class S3UnpackerTest extends UnpackerTestCases[Bucket] with S3Fixtures {
  override val unpacker: Unpacker = new S3Unpacker()

  override def withNamespace[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  // TODO: Add covariance to StreamStore
  override def withStreamStore[R](
    testWith: TestWith[StreamStore[ObjectLocation, InputStreamWithLength], R]
  ): R = {
    val s3StreamStore = new S3StreamStore()

    val store = new StreamStore[ObjectLocation, InputStreamWithLength] {
      override def get(location: ObjectLocation): ReadEither =
        s3StreamStore
          .get(location)
          .map { is =>
            Identified(
              is.id,
              new InputStreamWithLength(
                is.identifiedT,
                length = is.identifiedT.length
              )
            )
          }

      override def put(
        location: ObjectLocation
      )(is: InputStreamWithLength): WriteEither =
        s3StreamStore
          .put(location)(
            new InputStreamWithLengthAndMetadata(
              is,
              length = is.length,
              metadata = Map.empty
            )
          )
          .map { is =>
            is.copy(
              identifiedT = new InputStreamWithLength(
                is.identifiedT,
                length = is.identifiedT.length
              )
            )
          }
    }

    testWith(store)
  }

  it("fails if asked to write to a non-existent bucket") {
    val (archiveFile, _, _) = createTgzArchiveWithRandomFiles()

    withLocalS3Bucket { srcBucket =>
      withStreamStore { implicit streamStore =>
        withArchive(srcBucket, archiveFile) { archiveLocation =>
          val dstLocation = createObjectLocationPrefixWith(
            namespace = createBucketName
          )
          val result =
            unpacker.unpack(
              ingestId = createIngestID,
              srcLocation = archiveLocation,
              dstLocation = dstLocation
            )

          val ingestResult = result.success.value
          ingestResult shouldBe a[IngestFailed[_]]
          ingestResult.summary.fileCount shouldBe 0
          ingestResult.summary.bytesUnpacked shouldBe 0

          val underlyingError =
            ingestResult.asInstanceOf[IngestFailed[UnpackSummary]]
          underlyingError.e shouldBe a[AmazonS3Exception]
          underlyingError.e.getMessage should startWith(
            "The specified bucket does not exist"
          )
        }
      }
    }
  }

  describe("includes users-facing messages if reading the archive fails") {
    it("if it gets a permissions error") {
      val (archiveFile, _, _) = createTgzArchiveWithRandomFiles()
      val dstLocation = createObjectLocationPrefix

      withLocalS3Bucket { srcBucket =>
        withStreamStore { implicit streamStore =>
          withArchive(srcBucket, archiveFile) { archiveLocation =>
            // These credentials are one of the preconfigured accounts in the
            // zenko s3server Docker image.
            // See https://s3-server.readthedocs.io/en/latest/DOCKER.html#scality-access-key-id-and-scality-secret-access-key
            // https://github.com/scality/cloudserver/blob/5e17ec8343cd181936616efc0ac8d19d06dcd97d/conf/authdata.json
            implicit val badS3Client: AmazonS3 =
              S3ClientFactory.create(
                region = "localhost",
                endpoint = "http://localhost:33333",
                accessKey = "accessKey2",
                secretKey = "verySecretKey2"
              )

            val badUnpacker: S3Unpacker =
              new S3Unpacker()(badS3Client)

            val result =
              badUnpacker.unpack(
                ingestId = createIngestID,
                srcLocation = archiveLocation,
                dstLocation = dstLocation
              )

            assertIsError(result) { maybeMessage =>
              maybeMessage.get shouldBe s"Access denied while trying to read s3://${archiveLocation.namespace}/${archiveLocation.path}"
            }
          }
        }
      }
    }

    it("if the bucket does not exist") {
      val srcLocation = createObjectLocationWith(bucket = createBucket)
      val dstLocation = createObjectLocationPrefix

      withStreamStore { implicit streamStore =>
        val result =
          unpacker.unpack(
            ingestId = createIngestID,
            srcLocation = srcLocation,
            dstLocation = dstLocation
          )

        assertIsError(result) { maybeMessage =>
          maybeMessage.get shouldBe s"There is no S3 bucket ${srcLocation.namespace}"
        }
      }
    }

    it("if the key does not exist") {
      val dstLocation = createObjectLocationPrefix

      withLocalS3Bucket { bucket =>
        val srcLocation = createObjectLocationWith(bucket = bucket)

        withStreamStore { implicit streamStore =>
          val result =
            unpacker.unpack(
              ingestId = createIngestID,
              srcLocation = srcLocation,
              dstLocation = dstLocation
            )

          assertIsError(result) { maybeMessage =>
            maybeMessage.get shouldBe s"There is no archive at s3://${srcLocation.namespace}/${srcLocation.path}"
          }
        }
      }
    }

    it("if the bucket name is invalid") {
      val srcLocation = createObjectLocationWith(namespace = "ABCD")
      val dstLocation = createObjectLocationPrefix

      withStreamStore { implicit streamStore =>
        val result =
          unpacker.unpack(
            ingestId = createIngestID,
            srcLocation = srcLocation,
            dstLocation = dstLocation
          )

        assertIsError(result) { maybeMessage =>
          maybeMessage.get shouldBe s"${srcLocation.namespace} is not a valid S3 bucket name"
        }
      }
    }
  }

  private def assertIsError(result: Try[IngestStepResult[UnpackSummary]])(
    checkMessage: Option[String] => Assertion
  ): Assertion = {
    val ingestResult = result.success.value
    ingestResult shouldBe a[IngestFailed[_]]

    val ingestFailed = ingestResult.asInstanceOf[IngestFailed[_]]

    checkMessage(ingestFailed.maybeUserFacingMessage)
  }
}
