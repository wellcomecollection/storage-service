package uk.ac.wellcome.platform.archive.bagunpacker.services


import java.io.FileInputStream

import org.apache.commons.io.IOUtils
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.bagunpacker.fixtures.CompressFixture
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3

import scala.concurrent.ExecutionContext.Implicits.global

class UnpackerServiceTest
  extends FunSpec
    with Matchers
    with ScalaFutures
    with CompressFixture
    with S3  {

    it("unpacks that bag!") {
      withLocalS3Bucket { srcBucket =>
        withLocalS3Bucket { dstBucket =>

          implicit val _ = s3Client

          val fileCount = 10

          val (archiveFile, files, expectedEntries) =
            createArchive(
              archiverName = "tar",
              compressorName = "gz",
              fileCount
            )

          val srcKey = archiveFile.getName
          val dstKey = "unpacked"

          val srcLocation = ObjectLocation(srcBucket.name, srcKey)
          val dstLocation = ObjectLocation(dstBucket.name, dstKey)

          s3Client.putObject(srcBucket.name, srcKey, archiveFile)

          println(
            s"Put ${archiveFile.getAbsolutePath} to s3://${srcBucket.name}/${srcKey}")

          val unpackService = new UnpackerService()
          val unpacking = unpackService
            .unpack(srcLocation, dstLocation)

          whenReady(unpacking) { unpacked =>
            val dstKeys = listKeysInBucket(dstBucket)
            val expectedBytes = files.foldLeft(0L)( (n,file) => {
              n + file.length()
            })

            println(s"Unpacked bytes: ${unpacked.bytesUnpacked}")
            println(s"Expected bytes: ${expectedBytes}")

            unpacked.fileCount shouldBe fileCount
            unpacked.bytesUnpacked shouldBe expectedBytes

            val actualFileMap = dstKeys.map { key =>
              val s3Object = s3Client.getObject(dstBucket.name, key)

              val content = IOUtils.toByteArray(s3Object.getObjectContent)

              val name = key
                .replaceFirst(s"$dstKey/", "")

              println(s"Found $key in ${dstBucket.name}")

              name -> content
            }.toMap

            actualFileMap.size shouldBe files.length

            files.map { file =>
              val fis = new FileInputStream(file)
              val content = IOUtils.toByteArray(fis)
              val name = relativeToTmpDir(file)

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
