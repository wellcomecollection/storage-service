package uk.ac.wellcome.platform.archive.bagunpacker.storage

import java.io.{File, FileInputStream, FileOutputStream, InputStream}

import org.apache.commons.compress.archivers.ArchiveEntry
import org.apache.commons.compress.utils.IOUtils
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.bagunpacker.fixtures.CompressFixture
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings

import scala.util.{Success, Try}

class ArchiveTest
    extends FunSpec
    with Matchers
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

    val result: Try[Set[ArchiveEntry]] =
      Archive.unpack(
        inputStream
      )(
        Set.empty[ArchiveEntry]
      )(fold)

    result shouldBe a[Success[_]]
    val unpacked = result.get

    unpacked.diff(expectedEntries) shouldBe Set.empty

    val expectedFiles = files
      .map(file => relativeToTmpDir(file) -> file)
      .toMap

    val actualFiles = unpacked
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
