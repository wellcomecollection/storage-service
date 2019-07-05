package uk.ac.wellcome.platform.archive.bagunpacker.services

import java.io.{File, FileInputStream}
import java.nio.file.Paths

import org.scalatest.{Assertion, FunSpec, Matchers, TryValues}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagunpacker.exceptions.ArchiveLocationException
import uk.ac.wellcome.platform.archive.bagunpacker.fixtures.CompressFixture
import uk.ac.wellcome.platform.archive.bagunpacker.models.UnpackSummary
import uk.ac.wellcome.platform.archive.common.storage.models.{IngestFailed, IngestStepSucceeded}
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.streaming.{InputStreamWithLength, StreamAssertions}
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}

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

  it("normalizes file entries such as './' when unpacking") {
    val (archiveFile, filesInArchive, _) =
      createTgzArchiveWithFiles(
        randomFilesWithNames(
          List("./testA", "/testB", "/./testC", "//testD")
        ))

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

  it("fails if the original archive does not exist") {
    val srcLocation = createObjectLocation
    val result =
      unpacker.unpack(
        ingestId = createIngestID,
        srcLocation = srcLocation,
        dstLocation = createObjectLocationPrefix
      )

    val ingestResult = result.success.value
    ingestResult shouldBe a[IngestFailed[_]]
    ingestResult.summary.fileCount shouldBe 0
    ingestResult.summary.bytesUnpacked shouldBe 0

    val underlyingError = ingestResult.asInstanceOf[IngestFailed[UnpackSummary]]
    underlyingError.e shouldBe a[ArchiveLocationException]
    underlyingError.e.getMessage should startWith(s"Error getting input stream for $srcLocation:")
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
