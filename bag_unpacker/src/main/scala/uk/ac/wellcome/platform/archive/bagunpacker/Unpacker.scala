package uk.ac.wellcome.platform.archive.bagunpacker


import java.io.{BufferedInputStream, InputStream}

import grizzled.slf4j.Logging
import org.apache.commons.compress.archivers.{ArchiveEntry, ArchiveInputStream, ArchiveStreamFactory}
import org.apache.commons.compress.compressors.CompressorStreamFactory

import scala.util.Try


object Unpacker extends Logging {

  def apply(
             inputStream: InputStream
           ): Either[Throwable, Stream[ArchiveEntry]] = {
    (for {
      compressorInputStream <- uncompress(
        new BufferedInputStream(inputStream)
      )

      archiveInputStream <- extract(
        new BufferedInputStream(compressorInputStream)
      )
    } yield entries(archiveInputStream)) toEither
  }

  private def entries(
                       inputStream: ArchiveInputStream
                     ): Stream[ArchiveEntry] =
    inputStream.getNextEntry match {
      case null => {
        inputStream.close()
        Stream.empty
      }
      case entry: ArchiveEntry => {
        entry #:: entries(inputStream)
      }
    }

  private def uncompress(input: InputStream) =
    Try(
      new CompressorStreamFactory()
        .createCompressorInputStream(input)
    )

  private def extract(input: InputStream) = Try(
    new ArchiveStreamFactory()
      .createArchiveInputStream(input)
  )
}