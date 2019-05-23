package uk.ac.wellcome.platform.archive.bagunpacker.services

import java.io.{File, FileInputStream}
import java.nio.file.Paths

import org.apache.commons.io.IOUtils
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.bagunpacker.exceptions.{
  ArchiveLocationException,
  UnpackerArchiveEntryUploadException
}
import uk.ac.wellcome.platform.archive.bagunpacker.fixtures.CompressFixture
import uk.ac.wellcome.platform.archive.bagunpacker.models.UnpackSummary
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestFailed,
  IngestStepSucceeded
}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3
import uk.ac.wellcome.storage.fixtures.S3.Bucket

import scala.concurrent.ExecutionContext.Implicits.global

class UnpackerTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with CompressFixture
    with RandomThings
    with S3 {

  val unpacker = Unpacker(
    s3Uploader = new S3Uploader()
  )

  it("unpacks a tgz archive") {
    withLocalS3Bucket { srcBucket =>
      withLocalS3Bucket { dstBucket =>
        val (archiveFile, filesInArchive, _) = createTgzArchiveWithRandomFiles()
        withArchive(srcBucket, archiveFile) { testArchive =>
          val dstKey = "unpacked"
          val dstLocation =
            ObjectLocation(dstBucket.name, dstKey)

          val summaryResult = unpacker
            .unpack(
              randomUUID.toString,
              testArchive,
              dstLocation
            )

          whenReady(summaryResult) { unpacked =>
            unpacked shouldBe a[IngestStepSucceeded[_]]

            val summary = unpacked.summary
            summary.fileCount shouldBe filesInArchive.size
            summary.bytesUnpacked shouldBe totalBytes(filesInArchive)

            assertBucketContentsMatchFiles(dstBucket, dstKey, filesInArchive)
          }
        }
      }
    }
  }

  it("normalizes file entries such as './' when unpacking") {
    withLocalS3Bucket { srcBucket =>
      withLocalS3Bucket { dstBucket =>
        val (archiveFile, filesInArchive, _) =
          createTgzArchiveWithFiles(
            randomFilesWithNames(
              List("./testA", "/testB", "/./testC", "//testD")
            ))
        withArchive(srcBucket, archiveFile) { testArchive =>
          val dstKey = "unpacked"
          val summaryResult = unpacker
            .unpack(
              randomUUID.toString,
              testArchive,
              ObjectLocation(dstBucket.name, dstKey)
            )

          whenReady(summaryResult) { unpacked =>
            unpacked shouldBe a[IngestStepSucceeded[_]]

            val summary = unpacked.summary
            summary.fileCount shouldBe filesInArchive.size
            summary.bytesUnpacked shouldBe totalBytes(filesInArchive)

            assertBucketContentsMatchFiles(dstBucket, dstKey, filesInArchive)
          }
        }
      }
    }
  }

  it("returns an IngestFailed if it cannot open the input stream") {
    val srcLocation = createObjectLocation
    val future =
      unpacker.unpack(
        randomUUID.toString,
        srcLocation = srcLocation,
        dstLocation = createObjectLocation
      )

    whenReady(future) { result =>
      result shouldBe a[IngestFailed[_]]
      result.summary.fileCount shouldBe 0
      result.summary.bytesUnpacked shouldBe 0
      val actualResult = result.asInstanceOf[IngestFailed[UnpackSummary]]
      actualResult.e shouldBe a[ArchiveLocationException]
      actualResult.e.getMessage should
        startWith(
          s"Error getting input stream for s3://$srcLocation: " +
            "The specified bucket is not valid")
    }
  }

  it("returns an IngestFailed if it cannot write to the destination") {
    withLocalS3Bucket { srcBucket =>
      val (archiveFile, _, _) = createTgzArchiveWithRandomFiles()
      withArchive(srcBucket, archiveFile) { testArchive =>
        val dstLocation = createObjectLocation
        val future =
          unpacker.unpack(
            randomUUID.toString,
            srcLocation = testArchive,
            dstLocation = dstLocation
          )

        whenReady(future) { result =>
          result shouldBe a[IngestFailed[_]]
          result.summary.fileCount shouldBe 0
          result.summary.bytesUnpacked shouldBe 0
          val actualResult = result.asInstanceOf[IngestFailed[UnpackSummary]]
          actualResult.e shouldBe a[UnpackerArchiveEntryUploadException]
          actualResult.e.getMessage should startWith("upload failed")
        }
      }
    }
  }

  private def assertBucketContentsMatchFiles(
    bucket: Bucket,
    keyStripPrefix: String,
    expectedFiles: List[File]): List[Any] = {
    val keys = listKeysInBucket(bucket)
    val locations = keys.map(ObjectLocation(bucket.name, _))
    val bucketFileMap = objectToContentMap(locations, keyStripPrefix)

    bucketFileMap.size shouldBe expectedFiles.length

    expectedFiles.map { actualArchiveFile =>
      val fis = new FileInputStream(actualArchiveFile)
      val content = IOUtils.toByteArray(fis)
      val archiveFileName = Paths
        .get(relativeToTmpDir(actualArchiveFile))
        .normalize()
        .toString

      val dstFile = bucketFileMap.get(archiveFileName)
      dstFile shouldBe defined

      val actualBytes = dstFile.get
      actualBytes.length shouldBe content.length
      actualBytes shouldEqual content
    }
  }

  private def objectToContentMap(
    objectLocations: List[ObjectLocation],
    stripKeyPrefix: String): Map[String, Array[Byte]] = {
    objectLocations.map { objectLocation: ObjectLocation =>
      val s3Object =
        s3Client.getObject(objectLocation.namespace, objectLocation.key)

      val content = IOUtils
        .toByteArray(s3Object.getObjectContent)

      val name = objectLocation.key
        .replaceFirst(
          s"$stripKeyPrefix/",
          ""
        )

      debug(s"Found $key in $objectLocation")

      name -> content
    }.toMap
  }

  private def totalBytes(files: List[File]) = {
    files
      .foldLeft(0L)((n, file) => {
        n + file.length()
      })
  }
}
