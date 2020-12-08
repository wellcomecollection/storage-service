package uk.ac.wellcome.platform.archive.bagunpacker.services

import java.io.{EOFException, File, FileInputStream}
import java.nio.file.Paths
import org.scalatest.{Assertion, EitherValues, TryValues}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagunpacker.fixtures.{
  CompressFixture,
  LocalResources
}
import uk.ac.wellcome.platform.archive.bagunpacker.models.UnpackSummary
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestFailed,
  IngestStepResult,
  IngestStepSucceeded
}
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.streaming.Codec._
import uk.ac.wellcome.storage.streaming.StreamAssertions
import uk.ac.wellcome.storage.{Location, Prefix}

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
    withNamespace { srcNamespace =>
      withStreamStore { implicit streamStore =>
        val srcLocation = createSrcLocationWith(namespace = srcNamespace)

        streamStore.put(srcLocation)(
          stringCodec.toStream("hello world").value
        ) shouldBe a[Right[_, _]]

        val result =
          withUnpacker {
            _.unpack(
              ingestId = createIngestID,
              srcLocation = srcLocation,
              dstPrefix = createDstPrefix
            )
          }

        assertIsError(result) {
          case (_, maybeUserFacingMessage) =>
            maybeUserFacingMessage.get should startWith(
              "Error trying to unpack the archive at"
            )
        }
      }
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
    withNamespace { srcNamespace =>
      withStreamStore { implicit streamStore =>
        val srcLocation = createSrcLocationWith(namespace = srcNamespace)

        val stream = getResource("/truncated.tar.gz")

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
              err shouldBe a[EOFException]
              maybeUserFacingMessage.get should startWith(
                "Unexpected EOF while unpacking the archive"
              )
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

  def assertIsError(result: Try[IngestStepResult[UnpackSummary[_, _]]])(
    checkMessage: (Throwable, Option[String]) => Assertion
  ): Assertion = {
    val ingestResult = result.success.value
    ingestResult shouldBe a[IngestFailed[_]]

    val ingestFailed = ingestResult.asInstanceOf[IngestFailed[_]]

    ingestResult.summary.fileCount shouldBe 0
    ingestResult.summary.bytesUnpacked shouldBe 0

    checkMessage(ingestFailed.e, ingestFailed.maybeUserFacingMessage)
  }
}
