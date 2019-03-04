package uk.ac.wellcome.platform.archive.bagunpacker.storage

import java.io.{BufferedInputStream, InputStream, OutputStream}

import org.apache.commons.compress.archivers.{
  ArchiveEntry,
  ArchiveInputStream,
  ArchiveStreamFactory
}
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.commons.compress.utils.IOUtils

import scala.util.{Failure, Success, Try}

object Unpack {

  def stream(inputStream: InputStream)(
    out: ArchiveEntry => OutputStream
  ): Stream[ArchiveEntry] = {

    val readArchive = new ReadArchive(inputStream)

    def entries(stream: InputStream): Stream[ArchiveEntry] = {
      readArchive.next(out) match {
        case None        => Stream.empty
        case Some(entry) => entry #:: entries(stream)
      }
    }

    entries(inputStream)
  }

  private class ReadArchive(inputStream: InputStream) {

    def next(
      out: ArchiveEntry => OutputStream
    ): Option[ArchiveEntry] = {

      unpack match {
        case Failure(t) => throw t
        case Success(archiveInputStream) => {
          archiveInputStream.getNextEntry match {
            case null => {
              archiveInputStream.close()
              inputStream.close()
              None
            }
            case entry: ArchiveEntry => {
              val outputStream = out(entry)

              IOUtils.copy(
                archiveInputStream,
                outputStream
              )

              outputStream.close()
              Some(entry)
            }
          }
        }
      }
    }

    def uncompress(input: InputStream) =
      Try(
        new CompressorStreamFactory()
          .createCompressorInputStream(input)
      )

    def extract(input: InputStream) = Try(
      new ArchiveStreamFactory()
        .createArchiveInputStream(input)
    )

    val unpack: Try[ArchiveInputStream] = for {
      compressorInputStream <- uncompress(
        new BufferedInputStream(inputStream)
      )

      archiveInputStream <- extract(
        new BufferedInputStream(compressorInputStream)
      )

    } yield archiveInputStream
  }
}
