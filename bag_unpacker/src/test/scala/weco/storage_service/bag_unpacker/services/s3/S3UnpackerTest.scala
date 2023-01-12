package weco.storage_service.bag_unpacker.services.s3

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.NoSuchBucketException
import weco.fixtures.TestWith
import weco.storage.fixtures.S3Fixtures.Bucket
import weco.storage.listing.s3.S3ObjectLocationListing
import weco.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import weco.storage.store.s3.S3StreamStore
import weco.storage_service.bag_unpacker.fixtures.s3.S3CompressFixture
import weco.storage_service.bag_unpacker.services.{Unpacker, UnpackerTestCases}

import java.io.IOException
import java.net.URI

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

  override def listKeysUnder(
    prefix: S3ObjectLocationPrefix
  )(implicit store: S3StreamStore): Seq[String] =
    S3ObjectLocationListing().list(prefix).value.toSeq.map { _.key }

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

          assertIsError(result) {
            case (exc, _) =>
              exc shouldBe a[NoSuchBucketException]
          }
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
            implicit val badS3Client: S3Client =
              S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(
                  AwsBasicCredentials.create("accessKey2", "verySecretKey2")))
                .forcePathStyle(true)
                .endpointOverride(new URI("http://localhost:33333"))
                .build()

            val badUnpacker: S3Unpacker = new S3Unpacker()(badS3Client)

            val result =
              badUnpacker.unpack(
                ingestId = createIngestID,
                srcLocation = archiveLocation,
                dstPrefix = dstPrefix
              )

            assertIsError(result) {
              case (_, maybeMessage) =>
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

        assertIsError(result) {
          case (_, maybeMessage) =>
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

          assertIsError(result) {
            case (_, maybeMessage) =>
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

        assertIsError(result) {
          case (_, maybeMessage) =>
            maybeMessage.get shouldBe s"${srcLocation.bucket} is not a valid S3 bucket name"
        }
      }
    }

    it("if there's an error parsing the header") {
      withLocalS3Bucket { srcBucket =>
        withLocalS3Bucket { dstBucket =>
          implicit val streamStore: S3StreamStore = new S3StreamStore()

          // This file was created with the following bash script:
          //
          //    for i in 1 2 3 4 5 6 7 8 9 10
          //    do
          //      dd if=/dev/urandom bs=8192 count=1 > "$i.bin"
          //    done
          //
          //    tar -cvf truncated_header.tar *.bin
          //    gzip truncated_header.tar
          //    python3 -c 'import os; os.truncate("truncated_header.tar.gz", 80000)'
          //
          // I found this particular error path by accident while trying to write
          // a regression test for https://github.com/wellcomecollection/platform/issues/4911
          //
          val stream = getResource("/truncated_header.tar.gz")

          val srcLocation = createS3ObjectLocationWith(srcBucket)
          streamStore.put(srcLocation)(stream) shouldBe a[Right[_, _]]

          val dstPrefix = createDstPrefixWith(dstBucket)

          val result =
            unpacker.unpack(
              ingestId = createIngestID,
              srcLocation = srcLocation,
              dstPrefix = dstPrefix
            )

          assertIsError(result) {
            case (err, maybeUserFacingMessage) =>
              maybeUserFacingMessage.get should startWith(
                "Unexpected EOF while unpacking the archive"
              )

              err shouldBe a[IOException]
          }
        }
      }
    }

    /** This is a regression test for issue #4911.
      *
      * In that issue, we were seeing an EOF error when unpacking a truncated tar.gz.
      * We have a test case for handling an EOF exception in UnpackerTestCases -- but
      * the S3 unpacker has a *second* source of EOF exceptions.
      *
      * The S3 unpacker reads a bag in "chunks".  (Using the LargeStreamReader class.)
      * It reads a fixed-size chunk, does any processing it needs, then reads the next
      * chunk, and so on.  This avoids holding the entire bag in memory, which could be
      * arbitrarily large.
      *
      * The EOF exception caught in UnpackerTestCases is caught when we *read* the
      * compressed bag.  We've read the whole thing into memory, and found it wanting.
      *
      * You can also get an EOF exception when you *write* a file from the bag.
      * In particular, if the S3 unpacker reads a good chunk and starts uploading a file,
      * then the EOF will be thrown by the uploader when it doesn't get enough bytes from
      * the next, bad chunk.
      *
      * To reproduce this, we have to ensure the final file falls in two different chunks.
      * We can do this by cranking down the size of each chunk.
      *
      * See https://github.com/wellcomecollection/platform/issues/4911
      *
      */
    it("if there's an EOF while writing an unpacked file ") {
      withLocalS3Bucket { srcBucket =>
        withLocalS3Bucket { dstBucket =>
          implicit val streamStore: S3StreamStore = new S3StreamStore()

          // This file was created with the following bash script:
          //
          //    for i in 1 2 3
          //    do
          //      dd if=/dev/urandom bs=131072 count=1 > "$i.bin"
          //    done
          //    tar -cvf truncated_s3.tar *.bin
          //    gzip truncated_s3.tar
          //    python3 -c 'import os; os.truncate("truncated_s3.tar.gz", 25000)'
          //
          val stream = getResource("/truncated_s3.tar.gz")

          val srcLocation = createS3ObjectLocationWith(srcBucket)
          streamStore.put(srcLocation)(stream) shouldBe a[Right[_, _]]

          val dstPrefix = createDstPrefixWith(dstBucket)

          val result =
            unpacker.unpack(
              ingestId = createIngestID,
              srcLocation = srcLocation,
              dstPrefix = dstPrefix
            )

          assertIsError(result) {
            case (_, maybeUserFacingMessage) =>
              maybeUserFacingMessage.get should startWith(
                "Unexpected EOF while unpacking the archive at"
              )
          }
        }
      }
    }
  }
}
