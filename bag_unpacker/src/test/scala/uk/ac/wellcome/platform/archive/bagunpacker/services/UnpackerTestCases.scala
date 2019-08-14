package uk.ac.wellcome.platform.archive.bagunpacker.services

import java.io.{File, FileInputStream}
import java.nio.file.Paths

import org.scalatest.{Assertion, FunSpec, Matchers, TryValues}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagunpacker.fixtures.CompressFixture
import uk.ac.wellcome.platform.archive.bagunpacker.models.UnpackSummary
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestFailed,
  IngestStepSucceeded
}
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.streaming.Codec._
import uk.ac.wellcome.storage.streaming.{
  InputStreamWithLength,
  StreamAssertions
}
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}

trait UnpackerTestCases[Namespace]
    extends FunSpec
    with Matchers
    with TryValues
    with CompressFixture[Namespace]
    with StreamAssertions {
  val unpacker: Unpacker

  def withNamespace[R](testWith: TestWith[Namespace, R]): R

  def withStreamStore[R](
    testWith: TestWith[StreamStore[ObjectLocation, InputStreamWithLength], R]
  ): R

  it("unpacks a tgz archive") {
    val (archiveFile, filesInArchive, _) = createTgzArchiveWithRandomFiles()

    withNamespace { srcNamespace =>
      withNamespace { dstNamespace =>
        withStreamStore { implicit streamStore =>
          withArchive(srcNamespace, archiveFile) { archiveLocation =>
            val dstLocation =
              createObjectLocationWith(dstNamespace, path = "unpacker").asPrefix

            val summaryResult = unpacker
              .unpack(
                ingestId = createIngestID,
                srcLocation = archiveLocation,
                dstPrefix = dstLocation
              )

            val unpacked = summaryResult.success.value
            unpacked shouldBe a[IngestStepSucceeded[_]]

            unpacked.maybeUserFacingMessage.get should fullyMatch regex
              """Unpacked \d+ bytes from \d+ files"""

            val summary = unpacked.summary
            summary.fileCount shouldBe filesInArchive.size
            summary.bytesUnpacked shouldBe totalBytes(filesInArchive)

            assertEqual(dstLocation, filesInArchive)
          }
        }
      }
    }
  }

  it("normalizes file entries such as './' when unpacking") {
    val (archiveFile, filesInArchive, _) =
      createTgzArchiveWithFiles(
        randomFilesWithNames(
          List("./testA", "/testB", "/./testC", "//testD")
        )
      )

    withNamespace { srcNamespace =>
      withNamespace { dstNamespace =>
        withStreamStore { implicit streamStore =>
          withArchive(srcNamespace, archiveFile) { archiveLocation =>
            val dstLocation =
              createObjectLocationWith(dstNamespace, path = "unpacker").asPrefix
            val summaryResult = unpacker
              .unpack(
                ingestId = createIngestID,
                srcLocation = archiveLocation,
                dstPrefix = dstLocation
              )

            val unpacked = summaryResult.success.value
            unpacked shouldBe a[IngestStepSucceeded[_]]

            unpacked.maybeUserFacingMessage.get should fullyMatch regex
              """Unpacked \d+ bytes from \d+ files"""

            val summary = unpacked.summary
            summary.fileCount shouldBe filesInArchive.size
            summary.bytesUnpacked shouldBe totalBytes(filesInArchive)

            assertEqual(dstLocation, filesInArchive)
          }
        }
      }
    }
  }

  it("fails if the original archive does not exist") {
    withNamespace { srcNamespace =>
      val srcLocation =
        createObjectLocationWith(srcNamespace, path = randomAlphanumeric)
      val result =
        unpacker.unpack(
          ingestId = createIngestID,
          srcLocation = srcLocation,
          dstPrefix = createObjectLocationPrefix
        )

      val ingestResult = result.success.value
      ingestResult shouldBe a[IngestFailed[_]]
      ingestResult.summary.fileCount shouldBe 0
      ingestResult.summary.bytesUnpacked shouldBe 0

      val ingestFailed = ingestResult.asInstanceOf[IngestFailed[UnpackSummary]]
      ingestFailed.maybeUserFacingMessage.get should startWith(
        "There is no archive at"
      )
    }
  }

  it("fails if the specified file is not in tar.gz format") {
    withNamespace { srcNamespace =>
      withStreamStore { implicit streamStore =>
        val srcLocation = createObjectLocationWith(
          namespace = srcNamespace,
          path = randomAlphanumeric
        )

        streamStore.put(srcLocation)(
          stringCodec.toStream("hello world").right.value
        ) shouldBe a[Right[_, _]]

        val result =
          unpacker.unpack(
            ingestId = createIngestID,
            srcLocation = srcLocation,
            dstPrefix = createObjectLocationPrefix
          )

        val ingestResult = result.success.value
        ingestResult shouldBe a[IngestFailed[_]]
        ingestResult.summary.fileCount shouldBe 0
        ingestResult.summary.bytesUnpacked shouldBe 0

        val ingestFailed =
          ingestResult.asInstanceOf[IngestFailed[UnpackSummary]]
        ingestFailed.maybeUserFacingMessage.get should startWith(
          s"Error trying to unpack the archive at"
        )
      }
    }
  }

  def assertEqual(prefix: ObjectLocationPrefix, expectedFiles: Seq[File])(
    implicit store: StreamStore[ObjectLocation, InputStreamWithLength]
  ): Seq[Assertion] = {
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
