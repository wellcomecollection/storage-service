package uk.ac.wellcome.platform.archive.bagunpacker.storage

import java.io._

import org.apache.commons.compress.archivers.ArchiveEntry
import org.apache.commons.io.IOUtils
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.bagunpacker.fixtures.CompressFixture
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.ingests.operation.OperationResult

import scala.concurrent.ExecutionContext.Implicits.global

class ArchiveTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with CompressFixture
    with RandomThings {

  it("should unpack a tar.gz file") {
    val (archiveFile, files, expectedEntries) =
      createTgzArchiveWithRandomFiles()

    val inputStream = new FileInputStream(archiveFile)

    val tmp = System.getProperty("java.io.tmpdir")

    val fold = (entries: Set[ArchiveEntry],
                inputStream: InputStream,
                entry: ArchiveEntry) => {
      val os = new FileOutputStream(
        new File(tmp, entry.getName)
      )

      IOUtils.copy(inputStream, os)

      entries + entry
    }

    val unpack =
      Archive.unpack(
        inputStream
      )(
        Set.empty[ArchiveEntry]
      )(fold)

    whenReady(unpack) { unpacked: OperationResult[Set[ArchiveEntry]] =>
      unpacked.summary.diff(expectedEntries) shouldBe Set.empty

      val expectedFiles = files
        .map(file => relativeToTmpDir(file) -> file)
        .toMap

      val actualFiles = unpacked.summary
        .map(entry => entry.getName -> new File(tmp, entry.getName))
        .toMap

      expectedFiles.foreach {
        case (key, expectedFile) => {
          val maybeActualFile = actualFiles.get(key)
          maybeActualFile shouldBe a[Some[_]]

          val actualFile = maybeActualFile.get

          actualFile.exists() shouldBe true

          val actualFis = new FileInputStream(actualFile)
          val actualBytes = IOUtils.toByteArray(actualFis)

          val expectedFis = new FileInputStream(expectedFile)
          val expectedBytes = IOUtils.toByteArray(expectedFis)

          actualBytes.length shouldBe expectedBytes.length
          actualBytes shouldEqual expectedBytes
        }
      }
    }
  }
}
