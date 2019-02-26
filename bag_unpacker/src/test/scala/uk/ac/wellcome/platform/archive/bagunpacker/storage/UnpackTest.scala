package uk.ac.wellcome.platform.archive.bagunpacker.storage

import java.io._
import java.util.UUID

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.bagunpacker.fixtures.CompressFixture
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings

import scala.io.Source

class UnpackTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with CompressFixture
    with RandomThings {

  it("should unpack a tar.gz file") {
    val (archiveFile, files, expectedEntries) =
      createArchive(
        archiverName = "tar",
        compressorName = "gz",
        fileCount = 10
      )

    val testUUID = UUID.randomUUID()

    val inputStream = new FileInputStream(archiveFile)

    val tmp = System.getProperty("java.io.tmpdir")

    val unpack = Unpack.stream(inputStream) { entry =>
      new FileOutputStream(
        new File(tmp, s"${entry.getName}-$testUUID")
      )
    }

    // Small stream so just process it!
    val actualEntries = unpack.toSet

    actualEntries.diff(expectedEntries) shouldBe Set.empty

    val expectedFiles = files
      .map(file => file.getName -> file)
      .toMap

    val actualFiles = actualEntries
      .map(entry =>
        entry.getName -> new File(tmp, s"${entry.getName}-$testUUID"))
      .toMap

    expectedFiles.foreach {
      case (key, expectedFile) => {
        val maybeActualFile = actualFiles.get(key)
        maybeActualFile shouldBe a[Some[_]]

        val actualFile = maybeActualFile.get

        actualFile.exists() shouldBe true

        val actualContents =
          Source.fromFile(actualFile).getLines().mkString

        val expectedContents =
          Source.fromFile(expectedFile).getLines().mkString

        actualContents shouldEqual (expectedContents)
      }
    }
  }
}
