package uk.ac.wellcome.platform.archive.bagunpacker.services

import java.io.{File, FileInputStream}
import java.nio.file.Paths

import org.scalatest.{Assertion, FunSpec, Matchers, TryValues}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagunpacker.fixtures.CompressFixture
import uk.ac.wellcome.platform.archive.bagunpacker.services.s3.S3Unpacker
import uk.ac.wellcome.platform.archive.common.storage.models.IngestStepSucceeded
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.store.s3.S3StreamStore
import uk.ac.wellcome.storage.streaming.{InputStreamWithLength, InputStreamWithLengthAndMetadata, StreamAssertions}
import uk.ac.wellcome.storage.{Identified, ObjectLocation, ObjectLocationPrefix}

trait UnpackerTestCases[Namespace] extends FunSpec with Matchers with TryValues with CompressFixture[Namespace] with StreamAssertions {
  val unpacker: Unpacker

  def withNamespace[R](testWith: TestWith[Namespace, R]): R

  def withStreamStore[R](testWith: TestWith[StreamStore[ObjectLocation, InputStreamWithLength], R]): R

  it("unpacks a tgz archive") {
    val (archiveFile, filesInArchive, _) = createTgzArchiveWithRandomFiles()

    withNamespace { srcNamespace =>
      withNamespace { dstNamespace =>
        withStreamStore { implicit streamStore =>
          withArchive(srcNamespace, archiveFile) { archiveLocation =>
            val dstLocation = createObjectLocationWith(dstNamespace, path = "unpacker").asPrefix

            val summaryResult = unpacker
              .unpack(
                ingestId = createIngestID,
                srcLocation = archiveLocation,
                dstLocation = dstLocation
              )

            val unpacked = summaryResult.success.value
            unpacked shouldBe a[IngestStepSucceeded[_]]

            val summary = unpacked.summary
            summary.fileCount shouldBe filesInArchive.size
            summary.bytesUnpacked shouldBe totalBytes(filesInArchive)

            assertEqual(dstLocation, filesInArchive)
          }
        }
      }
    }
  }

  def assertEqual(prefix: ObjectLocationPrefix, expectedFiles: Seq[File])(implicit store: StreamStore[ObjectLocation, InputStreamWithLength]): Seq[Assertion] = {
    expectedFiles.map { file =>
      val name = Paths
        .get(relativeToTmpDir(file))
        .normalize()
        .toString

      val expectedLocation = prefix.asLocation(name)

      val originalContent = new FileInputStream(file)
      val storedContent = store.get(expectedLocation).right.value.identifiedT

      assertStreamsEqual(originalContent, storedContent)
    }
  }

  private def totalBytes(files: Seq[File]): Long =
    files
      .foldLeft(0L) { (n, file) =>
        n + file.length()
      }
}

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
}

//class UnpackerTest
//    extends FunSpec
//    with Matchers
//    with CompressFixture
//    with StorageRandomThings
//    with TryValues
//    with S3Fixtures {
//
//  val unpacker = Unpacker(
//    downloader = new S3StreamStore(),
//    s3Uploader = new S3Uploader()
//  )
//
//
//  it("normalizes file entries such as './' when unpacking") {
//    withLocalS3Bucket { srcBucket =>
//      withLocalS3Bucket { dstBucket =>
//        val (archiveFile, filesInArchive, _) =
//          createTgzArchiveWithFiles(
//            randomFilesWithNames(
//              List("./testA", "/testB", "/./testC", "//testD")
//            ))
//        withArchive(srcBucket, archiveFile) { testArchive =>
//          val dstKey = "unpacked"
//          val summaryResult = unpacker
//            .unpack(
//              ingestId = createIngestID,
//              srcLocation = testArchive,
//              dstLocation = ObjectLocation(dstBucket.name, dstKey)
//            )
//
//          val unpacked = summaryResult.success.value
//          unpacked shouldBe a[IngestStepSucceeded[_]]
//
//          val summary = unpacked.summary
//          summary.fileCount shouldBe filesInArchive.size
//          summary.bytesUnpacked shouldBe totalBytes(filesInArchive)
//
//          assertBucketContentsMatchFiles(dstBucket, dstKey, filesInArchive)
//        }
//      }
//    }
//  }
//
//  it("returns an IngestFailed if it cannot open the input stream") {
//    val srcLocation = createObjectLocation
//    val result =
//      unpacker.unpack(
//        ingestId = createIngestID,
//        srcLocation = srcLocation,
//        dstLocation = createObjectLocation
//      )
//
//    val ingestResult = result.success.value
//    ingestResult shouldBe a[IngestFailed[_]]
//    ingestResult.summary.fileCount shouldBe 0
//    ingestResult.summary.bytesUnpacked shouldBe 0
//
//    val underlyingError = ingestResult.asInstanceOf[IngestFailed[UnpackSummary]]
//    underlyingError.e shouldBe a[ArchiveLocationException]
//    underlyingError.e.getMessage should
//      startWith(
//        s"Error getting input stream for s3://$srcLocation: " +
//          "No such object")
//  }
//
//  it("returns an IngestFailed if it cannot write to the destination") {
//    withLocalS3Bucket { srcBucket =>
//      val (archiveFile, _, _) = createTgzArchiveWithRandomFiles()
//      withArchive(srcBucket, archiveFile) { testArchive =>
//        val dstLocation = createObjectLocation
//        val result =
//          unpacker.unpack(
//            ingestId = createIngestID,
//            srcLocation = testArchive,
//            dstLocation = dstLocation
//          )
//
//        val ingestResult = result.success.value
//        ingestResult shouldBe a[IngestFailed[_]]
//        ingestResult.summary.fileCount shouldBe 0
//        ingestResult.summary.bytesUnpacked shouldBe 0
//
//        val underlyingError =
//          ingestResult.asInstanceOf[IngestFailed[UnpackSummary]]
//        underlyingError.e shouldBe a[Throwable]
//        underlyingError.e.getMessage should startWith("Error from S3StreamStore")
//      }
//    }
//  }
//
//  private def assertBucketContentsMatchFiles(
//    bucket: Bucket,
//    keyStripPrefix: String,
//    expectedFiles: Seq[File]): Seq[Assertion] = {
//    val keys = listKeysInBucket(bucket)
//    val locations = keys.map(ObjectLocation(bucket.name, _))
//    val bucketFileMap = objectToContentMap(locations, keyStripPrefix)
//
//    bucketFileMap.size shouldBe expectedFiles.length
//
//    expectedFiles.map { actualArchiveFile =>
//      val fis = new FileInputStream(actualArchiveFile)
//      val content = IOUtils.toByteArray(fis)
//      val archiveFileName = Paths
//        .get(relativeToTmpDir(actualArchiveFile))
//        .normalize()
//        .toString
//
//      val dstFile = bucketFileMap.get(archiveFileName)
//      dstFile shouldBe defined
//
//      val actualBytes = dstFile.get
//      actualBytes.length shouldBe content.length
//      actualBytes shouldEqual content
//    }
//  }
//
//  private def objectToContentMap(
//    objectLocations: List[ObjectLocation],
//    stripKeyPrefix: String): Map[String, Array[Byte]] = {
//    objectLocations.map { objectLocation: ObjectLocation =>
//      val s3Object =
//        s3Client.getObject(objectLocation.namespace, objectLocation.path)
//
//      val content = IOUtils
//        .toByteArray(s3Object.getObjectContent)
//
//      val name = objectLocation.path
//        .replaceFirst(
//          s"$stripKeyPrefix/",
//          ""
//        )
//
//      debug(s"Found $key in $objectLocation")
//
//      name -> content
//    }.toMap
//  }
//
