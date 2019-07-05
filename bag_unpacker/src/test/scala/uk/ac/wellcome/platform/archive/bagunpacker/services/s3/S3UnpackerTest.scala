package uk.ac.wellcome.platform.archive.bagunpacker.services.s3

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagunpacker.models.UnpackSummary
import uk.ac.wellcome.platform.archive.bagunpacker.services.{Unpacker, UnpackerTestCases}
import uk.ac.wellcome.platform.archive.common.storage.models.IngestFailed
import uk.ac.wellcome.storage.{Identified, ObjectLocation}
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.store.s3.S3StreamStore
import uk.ac.wellcome.storage.streaming.{InputStreamWithLength, InputStreamWithLengthAndMetadata}

class S3UnpackerTest extends UnpackerTestCases[Bucket] with S3Fixtures {
  override val unpacker: Unpacker = new S3Unpacker()

  override def withNamespace[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  // TODO: Add covariance to StreamStore
  override def withStreamStore[R](testWith: TestWith[StreamStore[ObjectLocation, InputStreamWithLength], R]): R = {
    val s3StreamStore = new S3StreamStore()

    val store = new StreamStore[ObjectLocation, InputStreamWithLength] {
      override def get(location: ObjectLocation): ReadEither =
        s3StreamStore.get(location)
          .map { is => Identified(is.id, new InputStreamWithLength(is.identifiedT, length = is.identifiedT.length)) }

      override def put(location: ObjectLocation)(is: InputStreamWithLength): WriteEither =
        s3StreamStore.put(location)(
          new InputStreamWithLengthAndMetadata(is, length = is.length, metadata = Map.empty)
        ).map { is =>
          is.copy(
            identifiedT = new InputStreamWithLength(is.identifiedT, length = is.identifiedT.length)
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
          val dstLocation = createObjectLocationPrefix
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
          underlyingError.e shouldBe a[Throwable]
          underlyingError.e.getMessage should startWith("The specified bucket is not valid")
        }
      }
    }
  }
}
