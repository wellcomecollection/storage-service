package uk.ac.wellcome.platform.archive.bagunpacker

import java.io._
import java.util.UUID

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings

import scala.io.Source

class UnpackTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with Compress
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

trait Compress extends RandomThings {
  def createArchive(
    archiverName: String,
    compressorName: String,
    fileCount: Int = 10
  ) = {

    val file = File.createTempFile(
      randomUUID.toString,
      ".tar.gz"
    )

    val fileOutputStream = new FileOutputStream(file)

    val archive = new Archive(
      archiverName,
      compressorName,
      fileOutputStream
    )

    val randomFiles = (1 to fileCount)
      .map(_ => randomFile(1024))

    val entries =
      randomFiles.map { randomFile =>
        archive.addFile(
          randomFile,
          randomFile.getName
        )
      } toSet

    archive.finish()
    fileOutputStream.close()

    (file, randomFiles, entries)
  }
}
