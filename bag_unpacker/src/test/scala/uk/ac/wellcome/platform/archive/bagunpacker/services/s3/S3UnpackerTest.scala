package uk.ac.wellcome.platform.archive.bagunpacker.services.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.AmazonS3Exception
import org.scalatest.Assertion
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagunpacker.fixtures.s3.S3CompressFixture
import uk.ac.wellcome.platform.archive.bagunpacker.models.UnpackSummary
import uk.ac.wellcome.platform.archive.bagunpacker.services.{
  Unpacker,
  UnpackerTestCases
}
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestFailed,
  IngestStepResult
}
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.listing.s3.S3ObjectLocationListing
import uk.ac.wellcome.storage.store.s3.S3StreamStore
import uk.ac.wellcome.storage.s3.{
  S3ClientFactory,
  S3ObjectLocation,
  S3ObjectLocationPrefix
}

import scala.util.Try

class S3UnpackerTest
    extends UnpackerTestCases[
      S3ObjectLocation,
      S3ObjectLocationPrefix,
      S3StreamStore,
      Bucket
    ]
    with S3CompressFixture {
  val unpacker: S3Unpacker = new S3Unpacker()

  override def withUnpacker[R](
    testWith: TestWith[
      Unpacker[S3ObjectLocation, S3ObjectLocation, S3ObjectLocationPrefix],
      R
    ]
  )(implicit store: S3StreamStore): R =
    testWith(unpacker)

  override def withNamespace[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  override def withStreamStore[R](testWith: TestWith[S3StreamStore, R]): R =
    testWith(new S3StreamStore())

  override def createSrcLocationWith(
    bucket: Bucket,
    key: String
  ): S3ObjectLocation =
    S3ObjectLocation(bucket = bucket.name, key = key)

  override def createDstPrefixWith(
    bucket: Bucket,
    keyPrefix: String
  ): S3ObjectLocationPrefix =
    S3ObjectLocationPrefix(bucket = bucket.name, keyPrefix = keyPrefix)

  override def listKeysUnder(prefix: S3ObjectLocationPrefix)(implicit store: S3StreamStore): Seq[String] =
    S3ObjectLocationListing().list(prefix).right.value.toSeq.map { _.key }

  it("fails if asked to write to a non-existent bucket") {
    val (archiveFile, _, _) = createTgzArchiveWithRandomFiles()

    withLocalS3Bucket { srcBucket =>
      withStreamStore { implicit streamStore =>
        withArchive(srcBucket, archiveFile) { archiveLocation =>
          val dstPrefix =
            createS3ObjectLocationPrefixWith(bucket = createBucket)
          val result =
            unpacker.unpack(
              ingestId = createIngestID,
              srcLocation = archiveLocation,
              dstPrefix = dstPrefix
            )

          val ingestResult = result.success.value
          ingestResult shouldBe a[IngestFailed[_]]
          ingestResult.summary.fileCount shouldBe 0
          ingestResult.summary.bytesUnpacked shouldBe 0

          val underlyingError =
            ingestResult.asInstanceOf[IngestFailed[UnpackSummary[_, _]]]
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
      val dstPrefix = createS3ObjectLocationPrefix

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
                dstPrefix = dstPrefix
              )

            assertIsError(result) { maybeMessage =>
              maybeMessage.get shouldBe s"Error reading $archiveLocation: either it doesn't exist, or the unpacker doesn't have permission to read it"
            }
          }
        }
      }
    }

    it("if the bucket does not exist") {
      val srcLocation = createS3ObjectLocationWith(bucket = createBucket)
      val dstPrefix = createS3ObjectLocationPrefix

      withStreamStore { implicit streamStore =>
        val result =
          unpacker.unpack(
            ingestId = createIngestID,
            srcLocation = srcLocation,
            dstPrefix = dstPrefix
          )

        assertIsError(result) { maybeMessage =>
          maybeMessage.get shouldBe s"There is no S3 bucket ${srcLocation.bucket}"
        }
      }
    }

    it("if the key does not exist") {
      val dstPrefix = createS3ObjectLocationPrefix

      withLocalS3Bucket { bucket =>
        val srcLocation = createS3ObjectLocationWith(bucket = bucket)

        withStreamStore { implicit streamStore =>
          val result =
            unpacker.unpack(
              ingestId = createIngestID,
              srcLocation = srcLocation,
              dstPrefix = dstPrefix
            )

          assertIsError(result) { maybeMessage =>
            maybeMessage.get shouldBe s"There is no archive at $srcLocation"
          }
        }
      }
    }

    it("if the bucket name is invalid") {
      val srcLocation = createS3ObjectLocationWith(bucket = createInvalidBucket)
      val dstPrefix = createS3ObjectLocationPrefix

      withStreamStore { implicit streamStore =>
        val result =
          unpacker.unpack(
            ingestId = createIngestID,
            srcLocation = srcLocation,
            dstPrefix = dstPrefix
          )

        assertIsError(result) { maybeMessage =>
          maybeMessage.get shouldBe s"${srcLocation.bucket} is not a valid S3 bucket name"
        }
      }
    }
  }

  private def assertIsError(result: Try[IngestStepResult[UnpackSummary[_, _]]])(
    checkMessage: Option[String] => Assertion
  ): Assertion = {
    val ingestResult = result.success.value
    ingestResult shouldBe a[IngestFailed[_]]

    val ingestFailed = ingestResult.asInstanceOf[IngestFailed[_]]

    checkMessage(ingestFailed.maybeUserFacingMessage)
  }
}
