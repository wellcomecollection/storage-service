package uk.ac.wellcome.platform.archive.bagunpacker.services

import org.scalatest.{FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures
import uk.ac.wellcome.platform.archive.bagunpacker.fixtures.CompressFixture
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3

import scala.io.Source

import scala.concurrent.ExecutionContext.Implicits.global

class UnpackServiceTest
  extends FunSpec
    with Matchers
    with ScalaFutures
    with CompressFixture
    with S3  {

    it("unpacks that bag!") {
      withLocalS3Bucket { bucket =>
        implicit val _ = s3Client

        val fileCount = 10

        val (archiveFile, files, expectedEntries) =
          createArchive(
            archiverName = "tar",
            compressorName = "gz",
            fileCount
          )

        val srcBucket = bucket.name
        val srcKey = archiveFile.getName

        val dstBucket = bucket.name
        val dstKey = "unpacked"

        val srcLocation = ObjectLocation(srcBucket, srcKey)
        val dstLocation = ObjectLocation(dstBucket, dstKey)

        s3Client.putObject(srcBucket, srcKey, archiveFile)

        val unpackService = new UnpackService()
        val unpacking = unpackService
          .unpack(srcLocation, dstLocation)

        whenReady(unpacking) { unpacked =>
          val keys = listKeysInBucket(bucket)

          unpacked should have size fileCount
          val unpackedKeys = keys.filter(_.startsWith(dstKey))

          val actualFileMap = unpackedKeys.map { key =>
            val s3Object = s3Client.getObject(bucket.name, key)

            val content = Source
              .fromInputStream(s3Object.getObjectContent)
              .mkString

            val name = key
              .replaceFirst(s"$dstKey/","")

            name -> content
          }.toMap

          val expectedFileMap = files.map { file =>
            val content = Source.fromFile(file).mkString

            val name = file.getName

            name -> content
          }.toMap

          actualFileMap shouldEqual expectedFileMap
        }
      }
    }
}
