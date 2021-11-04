package weco.storage_service.bag_unpacker.services

import java.io.{File, FileInputStream}
import java.nio.file.Paths
import org.scalatest.{Assertion, EitherValues, TryValues}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.fixtures.TestWith
import weco.storage_service.bag_unpacker.fixtures.{
  CompressFixture,
  LocalResources
}
import weco.storage_service.bag_unpacker.models.UnpackSummary
import weco.storage_service.storage.models.{
  IngestFailed,
  IngestStepResult,
  IngestStepSucceeded
}
import weco.storage.store.StreamStore
import weco.storage.streaming.StreamAssertions
import weco.storage.{Location, Prefix}

import scala.util.Try

trait UnpackerTestCases[BagLocation <: Location, BagPrefix <: Prefix[
  BagLocation
], StoreImpl <: StreamStore[BagLocation], Namespace]
    extends AnyFunSpec
    with Matchers
    with EitherValues
    with TryValues
    with CompressFixture[BagLocation, Namespace]
    with StreamAssertions
    with LocalResources {
  def withUnpacker[R](
    testWith: TestWith[Unpacker[BagLocation, BagLocation, BagPrefix], R]
  )(
    implicit streamStore: StoreImpl
  ): R

  private def withUnpackerAndStore[R](
    testWith: TestWith[Unpacker[BagLocation, BagLocation, BagPrefix], R]
  ): R =
    withStreamStore { implicit streamStore =>
      withUnpacker { unpacker =>
        testWith(unpacker)
      }
    }

  def withNamespace[R](testWith: TestWith[Namespace, R]): R

  def withStreamStore[R](testWith: TestWith[StoreImpl, R]): R

  def createSrcLocationWith(
    namespace: Namespace,
    path: String = randomAlphanumeric()
  ): BagLocation

  def createDstPrefixWith(
    namespace: Namespace,
    pathPrefix: String = randomAlphanumeric()
  ): BagPrefix

  override def createLocationWith(
    namespace: Namespace,
    path: String
  ): BagLocation =
    createSrcLocationWith(namespace = namespace, path = path)

  def createDstPrefix: BagPrefix =
    withNamespace { namespace =>
      createDstPrefixWith(namespace)
    }

  def listKeysUnder(prefix: BagPrefix)(implicit store: StoreImpl): Seq[String]

  it("unpacks a tgz archive") {
    val (archiveFile, filesInArchive, _) = createTgzArchiveWithRandomFiles()

    withNamespace { srcNamespace =>
      withNamespace { dstNamespace =>
        withStreamStore { implicit streamStore =>
          withArchive(srcNamespace, archiveFile) { archiveLocation =>
            val dstPrefix =
              createDstPrefixWith(dstNamespace, pathPrefix = "unpacker")

            val summaryResult =
              withUnpacker {
                _.unpack(
                  ingestId = createIngestID,
                  srcLocation = archiveLocation,
                  dstPrefix = dstPrefix
                )
              }

            val unpacked = summaryResult.success.value
            unpacked shouldBe a[IngestStepSucceeded[_]]

            unpacked.maybeUserFacingMessage.get should fullyMatch regex
              """Unpacked \d+ [KM]B from \d+ files"""

            val summary = unpacked.summary
            summary.fileCount shouldBe filesInArchive.size
            summary.bytesUnpacked shouldBe totalBytes(filesInArchive)

            assertEqual(dstPrefix, filesInArchive)
          }
        }
      }
    }
  }

  it("normalizes file entries such as './' when unpacking") {
    // This is a stripped down copy of a bag which got unpacked into a prefix
    // containing ./ in the S3 key.
    // See ingest bd5cef81-ea38-4542-b0af-871d70f8d6bf
    val inputStream = getResource("/dotted_archive.tar.gz")

    withNamespace { srcNamespace =>
      withNamespace { dstNamespace =>
        withStreamStore { implicit streamStore =>
          val srcLocation = createSrcLocationWith(srcNamespace)
          streamStore.put(srcLocation)(inputStream) shouldBe a[Right[_, _]]

          val dstPrefix =
            createDstPrefixWith(dstNamespace, pathPrefix = "unpacker")

          val summaryResult =
            withUnpacker {
              _.unpack(
                ingestId = createIngestID,
                srcLocation = srcLocation,
                dstPrefix = dstPrefix
              )
            }

          val unpacked = summaryResult.success.value
          unpacked shouldBe a[IngestStepSucceeded[_]]

          listKeysUnder(dstPrefix) should contain theSameElementsAs Seq(
            "unpacker/PBLBIO_A_2_1-5adb2889-8556-49f8-a386-440a1d8f57f1/bag-info.txt",
            "unpacker/PBLBIO_A_2_1-5adb2889-8556-49f8-a386-440a1d8f57f1/bagit.txt",
            "unpacker/PBLBIO_A_2_1-5adb2889-8556-49f8-a386-440a1d8f57f1/data/README.html",
            "unpacker/PBLBIO_A_2_1-5adb2889-8556-49f8-a386-440a1d8f57f1/manifest-sha256.txt",
            "unpacker/PBLBIO_A_2_1-5adb2889-8556-49f8-a386-440a1d8f57f1/tagmanifest-sha256.txt"
          )
        }
      }
    }
  }

  it("fails if the original archive does not exist") {
    withNamespace { srcNamespace =>
      val srcLocation = createSrcLocationWith(srcNamespace)
      val result =
        withUnpackerAndStore {
          _.unpack(
            ingestId = createIngestID,
            srcLocation = srcLocation,
            dstPrefix = createDstPrefix
          )
        }

      assertIsError(result) {
        case (_, maybeUserFacingMessage) =>
          maybeUserFacingMessage.get should startWith("There is no archive at")
      }
    }
  }

  it("fails if the specified file is not in tar.gz format") {
    assertUnpackingFailsWith("/greeting.txt") {
      case (_, _, userFacingMessage) =>
        userFacingMessage should startWith(
          "Error trying to unpack the archive at"
        )
    }
  }

  /** The file for this test was created with the following bash script:
    *
    *     for i in 1 2 3
    *     do
    *       dd if=/dev/urandom bs=4096 count=1 > "$i.bin"
    *     done
    *
    *     tar -cvf truncated.tar *.bin
    *     gzip truncated.tar
    *     python3 -c 'import os; os.truncate("truncated.tar.gz", 10000)'
    *
    * The sizes were chosen somewhat experimentally to get a bag that caused
    * an EOF error in the Unarchiver midway through the archive.
    *
    * It was built to replicate an issue seen from a bag encountered in prod.
    * This bag was created when Archivematica tried to upload a bag on a system
    * that was running out of disk space.
    * See: https://github.com/wellcomecollection/platform/issues/4911
    *
    */
  it("fails if the archive has an EOF error") {
    assertUnpackingFailsWith("/truncated.tar.gz") {
      case (_, _, userFacingMessage) =>
        userFacingMessage should startWith(
          "Unexpected EOF while unpacking the archive"
        )
    }
  }

  /** The file for this test is based on a real test file, which was sent
    * in ingest bfe7e4f3-77af-4d2f-8d4c-9d9b31ed7e4d
    *
    * The goal here is that the file should have the correct gzip compression,
    * but the inner tarball should be incomplete.
    *
    * The test file was created by reducing the original file by hand -- that is,
    * deleting or replacing bytes in a hex editor while confirming that we continue
    * to get the same error.  We can't use the original file in the test/repo because
    * (1) it's ~11GB in size and (2) it contains copyrighted images.
    *
    * We're not sure, but we think this occurred when Archivematica restarted
    * midway through uploading a bag.
    *
    */
  it("fails if the uncompressed tarball has an EOF error") {
    assertUnpackingFailsWith("/truncated_tar.tar.gz") {
      case (_, _, userFacingMessage) =>
        userFacingMessage should startWith(
          "Unexpected EOF while unpacking the archive"
        )
    }
  }

  /** The file for this test was created with the following bash script:
    *
    *     dd if=/dev/urandom bs=1024 count=1 > 1.bin
    *     tar -cvf repetitive.tar 1.bin 1.bin
    *     gzip repetitive.tar
    *
    * I discovered this failure mode accidentally while experimenting with
    * test cases for issue #4911, but it's unrelated to that issue.
    *
    */
  it("fails if the archive has repeated entries") {
    assertUnpackingFailsWith("/repetitive.tar.gz") {
      case (srcLocation, _, userFacingMessage) =>
        userFacingMessage shouldBe s"The archive at $srcLocation is malformed or has a duplicate entry (1.bin)"
    }
  }

  /** The file for this test was created with the following bash script:
    *
    *     mkdir -p truncated_crc32
    *
    *     for i in $(seq 0 9); do
    *       echo "$i" > truncated_crc32/"$i"
    *     done
    *
    *     tar -czvf truncated_crc32.tar.gz truncated_crc32
    *
    *     python3 -c 'b = open("truncated_crc32.tar.gz", "rb").read(); b = b[:-1] + b"\xff"; open("truncated_crc32.tar.gz", "wb").write(b)'
    *
    * The Python command changes the final bit of the file to 0xff (when I was
    * testing, the correct byte here was always 0x00).
    */
  it("fails if the gzip-compressed data is corrupt") {
    assertUnpackingFailsWith("/truncated_crc32.tar.gz") {
      case (_, _, userFacingMessage) =>
        userFacingMessage should startWith(
          "Error trying to unpack the archive at"
        )
        userFacingMessage should endWith("is the gzip-compression correct?")
    }
  }

  protected def assertUnpackingFailsWith[R](
    filename: String
  )(testWith: TestWith[(BagLocation, Throwable, String), R]): R = {
    withNamespace { srcNamespace =>
      withStreamStore { implicit streamStore =>
        val srcLocation = createSrcLocationWith(namespace = srcNamespace)

        val stream = getResource(filename)

        streamStore.put(srcLocation)(stream) shouldBe a[Right[_, _]]

        withNamespace { dstNamespace =>
          val result =
            withUnpacker {
              _.unpack(
                ingestId = createIngestID,
                srcLocation = srcLocation,
                dstPrefix = createDstPrefixWith(dstNamespace)
              )
            }

          assertIsError(result) {
            case (err, maybeUserFacingMessage) =>
              testWith((srcLocation, err, maybeUserFacingMessage.get))
          }
        }
      }
    }
  }

  def assertEqual(prefix: BagPrefix, expectedFiles: Seq[File])(
    implicit store: StreamStore[BagLocation]
  ): Seq[Assertion] = {
    expectedFiles.map { file =>
      val name = Paths
        .get(relativeToTmpDir(file))
        .normalize()
        .toString

      val expectedLocation = prefix.asLocation(name)

      val originalContent = new FileInputStream(file)
      val storedContent = store.get(expectedLocation).value.identifiedT

      assertStreamsEqual(originalContent, storedContent)
    }
  }

  private def totalBytes(files: Seq[File]): Long =
    files
      .foldLeft(0L) { (n, file) =>
        n + file.length()
      }

  def assertIsError[R](result: Try[IngestStepResult[UnpackSummary[_, _]]])(
    checkMessage: (Throwable, Option[String]) => R
  ): R = {
    val ingestResult = result.success.value
    ingestResult shouldBe a[IngestFailed[_]]

    val ingestFailed = ingestResult.asInstanceOf[IngestFailed[_]]

    ingestResult.summary.fileCount shouldBe 0
    ingestResult.summary.bytesUnpacked shouldBe 0

    checkMessage(ingestFailed.e, ingestFailed.maybeUserFacingMessage)
  }
}
