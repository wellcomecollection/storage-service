package uk.ac.wellcome.platform.archive.bagunpacker

import java.io._

import org.apache.commons.compress.archivers.{
  ArchiveOutputStream,
  ArchiveStreamFactory
}
import org.apache.commons.compress.compressors.{
  CompressorOutputStream,
  CompressorStreamFactory
}
import org.apache.commons.io.IOUtils

import scala.concurrent.Future
import scala.util.Try

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

  def finish() = synchronized {
    Future.fromTry(Try {
      archiveOutputStream.flush()
      archiveOutputStream.finish()
      compressorOutputStream.flush()
      compressorOutputStream.close()
    })
  }

  def addFile(
    file: File,
    entryName: String
  ) = synchronized {
    Future.fromTry(Try {

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
    })
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
