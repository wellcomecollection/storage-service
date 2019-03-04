package uk.ac.wellcome.platform.archive.bagunpacker.fixtures

import java.io._

import grizzled.slf4j.Logging
import org.apache.commons.compress.archivers.{ArchiveOutputStream, ArchiveStreamFactory}
import org.apache.commons.compress.compressors.{CompressorOutputStream, CompressorStreamFactory}
import org.apache.commons.io.IOUtils
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings

trait CompressFixture extends RandomThings with Logging {
  def createArchive(
    archiverName: String,
    compressorName: String,
    fileCount: Int = 100
  ) = {

    val file = File.createTempFile(
      randomUUID.toString,
      ".test"
    )

    val fileOutputStream =
      new FileOutputStream(file)

    val archive = new Archive(
      archiverName,
      compressorName,
      fileOutputStream
    )

    val randomFiles =
      randomFilesInDirs(
        fileCount,
        fileCount/4
      )

    val entries =
      randomFiles.map { randomFile =>
        println(s"Archiving ${randomFile.getAbsolutePath} in ${file.getAbsolutePath}")
        archive.addFile(
          randomFile,
          relativeToTmpDir(randomFile)
        )
      } toSet

    archive.finish()
    fileOutputStream.close()

    (file, randomFiles, entries)
  }

  def relativeToTmpDir(file: File) = (new File(tmpDir).toURI)
    .relativize(file.toURI)
    .getPath

  class Archive(
    archiverName: String,
    compressorName: String,
    outputStream: OutputStream
  ) {

    private val compress = compressor(compressorName)(_)
    private val pack = packer(archiverName)(_)

    private val compressorOutputStream =
      compress(outputStream)

    private val archiveOutputStream = pack(
      compressorOutputStream
    )

    def finish() = {
      archiveOutputStream.flush()
      archiveOutputStream.finish()
      compressorOutputStream.flush()
      compressorOutputStream.close()
    }

    def addFile(
      file: File,
      entryName: String
    ) = {
      synchronized {

        val entry = archiveOutputStream
          .createArchiveEntry(file, entryName)

        val fileInputStream = new BufferedInputStream(
          new FileInputStream(file)
        )

        archiveOutputStream.putArchiveEntry(entry)

        IOUtils.copy(
          fileInputStream,
          archiveOutputStream
        )

        archiveOutputStream.closeArchiveEntry()

        fileInputStream.close()

        entry
      }
    }

    private def compressor(
      compressorName: String
    )(
      outputStream: OutputStream
    ): CompressorOutputStream = {

      val compressorStreamFactory =
        new CompressorStreamFactory()

      val bufferedOutputStream =
        new BufferedOutputStream(outputStream)

      compressorStreamFactory
        .createCompressorOutputStream(
          compressorName,
          bufferedOutputStream
        )
    }

    private def packer(
      archiverName: String
    )(
      outputStream: OutputStream
    ): ArchiveOutputStream = {

      val archiveStreamFactory =
        new ArchiveStreamFactory()

      val bufferedOutputStream =
        new BufferedOutputStream(outputStream)

      archiveStreamFactory
        .createArchiveOutputStream(
          archiverName,
          bufferedOutputStream
        )

    }
  }
}
