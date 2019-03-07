package uk.ac.wellcome.platform.archive.bagunpacker.services

import java.io.{File, FileInputStream}
import java.nio.file.Paths

import org.apache.commons.io.IOUtils
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.bagunpacker.fixtures.CompressFixture
import uk.ac.wellcome.platform.archive.common.operation.{
  OperationFailure,
  OperationSuccess
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
    with S3 {

  val unpacker = new Unpacker()

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
              testArchive,
              dstLocation
            )

          whenReady(summaryResult) { unpacked =>
            unpacked shouldBe a[OperationSuccess[_]]

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
              testArchive,
              ObjectLocation(dstBucket.name, dstKey)
            )

          whenReady(summaryResult) { unpacked =>
            unpacked shouldBe a[OperationSuccess[_]]

            val summary = unpacked.summary
            summary.fileCount shouldBe filesInArchive.size
            summary.bytesUnpacked shouldBe totalBytes(filesInArchive)

            assertBucketContentsMatchFiles(dstBucket, dstKey, filesInArchive)
          }
        }
      }
    }
  }

  it("returns a failed OperationResult if it cannot open the input stream") {
    val future = unpacker
      .unpack(
        srcLocation = createObjectLocation,
        dstLocation = createObjectLocation
      )

    whenReady(future) { result =>
      result shouldBe a[OperationFailure[_]]
    }
  }

  it("returns a failed OperationResult if it cannot write to the destination") {
    withLocalS3Bucket { srcBucket =>
      val (archiveFile, _, _) = createTgzArchiveWithRandomFiles()
      withArchive(srcBucket, archiveFile) { testArchive =>
        val future = unpacker
          .unpack(
            srcLocation = testArchive,
            dstLocation = createObjectLocation
          )

        whenReady(future) { unpacked =>
          unpacked shouldBe a[OperationFailure[_]]
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

      println(s"Found $key in $objectLocation")

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
