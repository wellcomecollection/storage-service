package uk.ac.wellcome.platform.archive.bagunpacker.services

import java.io.FileInputStream
import java.nio.file.Paths

import org.apache.commons.io.IOUtils
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.bagunpacker.fixtures.CompressFixture
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3

import scala.concurrent.ExecutionContext.Implicits.global

class UnpackerTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with CompressFixture
    with S3 {

  it("unpacks a bag") {
    withLocalS3Bucket { srcBucket =>
      withLocalS3Bucket { dstBucket =>
        withArchive(srcBucket) { testArchive =>
          val fileCount = defaultFileCount
          val dstKey = "unpacked"
          val dstLocation =
            ObjectLocation(dstBucket.name, dstKey)

          val unpackService = new Unpacker()

          val unpacking = unpackService
            .unpack(
              testArchive.location,
              dstLocation
            )

          whenReady(unpacking) { unpacked =>
            val summary = unpacked.summary
            val dstKeys = listKeysInBucket(dstBucket)
            val expectedBytes =
              testArchive.containedFiles
                .foldLeft(0L)((n, file) => {
                  n + file.length()
                })

            println(s"Unpacked bytes: ${summary.bytesUnpacked}")
            println(s"Expected bytes: ${expectedBytes}")

            summary.fileCount shouldBe fileCount
            summary.bytesUnpacked shouldBe expectedBytes

            val actualFileMap = dstKeys.map { key =>
              val s3Object = s3Client
                .getObject(dstBucket.name, key)

              val content = IOUtils
                .toByteArray(s3Object.getObjectContent)

              val name = key
                .replaceFirst(
                  s"$dstKey/",
                  ""
                )

              println(s"Found $key in ${dstBucket.name}")

              name -> content
            }.toMap

            actualFileMap.size shouldBe testArchive.containedFiles.length

            testArchive.containedFiles.map { file =>
              val fis = new FileInputStream(file)
              val content = IOUtils.toByteArray(fis)
              val name = Paths.get(relativeToTmpDir(file))
                .normalize().toString
              
              val actualFile = actualFileMap.get(name)

              actualFile shouldBe defined
              val actualBytes = actualFile.get

              actualBytes.length shouldBe content.length
              actualBytes shouldEqual content
            }
          }
        }
      }
    }
  }
}
