package uk.ac.wellcome.platform.archive.bagunpacker.storage

import java.io.{BufferedInputStream, InputStream}

import grizzled.slf4j.Logging
import org.apache.commons.compress.archivers.{ArchiveEntry, ArchiveInputStream, ArchiveStreamFactory}
import org.apache.commons.compress.compressors.{CompressorInputStream, CompressorStreamFactory}
import org.apache.commons.io.input.CloseShieldInputStream

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object BetterArchive {
  def unpack(inputStream: InputStream): Try[Iterator[(ArchiveEntry, InputStream)]] =
    for {
      uncompressedStream <- uncompress(inputStream)
      archiveInputStream: ArchiveInputStream <- extract(uncompressedStream)
      iterator = createIterator(archiveInputStream)
    } yield iterator

  private def createIterator(archiveInputStream: ArchiveInputStream): Iterator[(ArchiveEntry, InputStream)] =
    new Iterator[(ArchiveEntry, InputStream)] {
      private var latest: ArchiveEntry = _

      override def hasNext: Boolean = {
        latest = archiveInputStream.getNextEntry
        latest != null
      }

      override def next(): (ArchiveEntry, InputStream) =
        (latest, archiveInputStream)
    }

  private def uncompress(compressedStream: InputStream): Try[CompressorInputStream] =
    Try {
      // We have to wrap in a BufferedInputStream because this method
      // only takes InputStreams that support the `mark()` method.
      new CompressorStreamFactory()
        .createCompressorInputStream(new BufferedInputStream(compressedStream))
    }

  private def extract(inputStream: InputStream) =
    Try {
      // We have to wrap in a BufferedInputStream because this method
      // only takes InputStreams that support the `mark()` method.
      new ArchiveStreamFactory()
        .createArchiveInputStream(new BufferedInputStream(inputStream))
    }
}

object Archive extends Logging {
  def unpack[T](
    inputStream: InputStream
  )(init: T)(
    f: (T, InputStream, ArchiveEntry) => T
  ): Try[T] = Try {
    val archiveReader = new ArchiveReader[T](inputStream)

    @tailrec
    def foldStream(stream: InputStream)(t: T)(
      f: (T, InputStream, ArchiveEntry) => T
    ): T = {

      archiveReader.accumulate(t, f) match {
        case StreamEnd(resultT) =>
          resultT
        case StreamContinues(resultT) =>
          foldStream(stream)(resultT)(f)
        case StreamError(_, e) =>
          throw e
      }
    }

    foldStream(inputStream)(init)(f)
  }

  private class ArchiveReader[T](inputStream: InputStream) {
    def accumulate(
      t: T,
      f: (T, InputStream, ArchiveEntry) => T,
    ): StreamStep[T] = {

      archiveInputStream match {
        case Failure(e) => StreamError(t, e)
        case Success(ais: ArchiveInputStream) => {
          ais.getNextEntry match {
            case null => {
              Try {
                ais.close()
                inputStream.close()
              } match {
                case Success(_) => StreamEnd(t)
                case Failure(e) => StreamError(t, e)
              }
            }

            case entry: ArchiveEntry => {
              // Some clients might (AWS S3 SDK does!)
              // dispose of our InputStream,
              // we want to stop it as the Unpacker
              // object requires it to remain open
              // to retrieve the next entry.

              Try {
                val closeShieldInputStream =
                  new CloseShieldInputStream(ais)

                f(t, closeShieldInputStream, entry)
              } match {
                case Success(r) => StreamContinues(r)
                case Failure(e) => StreamError(t, e)
              }
            }
          }
        }
      }
    }

    val archiveInputStream: Try[ArchiveInputStream] = for {
      compressorInputStream <- uncompress(
        new BufferedInputStream(inputStream)
      )

      archiveInputStream <- extract(
        new BufferedInputStream(compressorInputStream)
      )

    } yield archiveInputStream

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

  private sealed trait StreamStep[T] {
    val t: T
  }

  private case class StreamError[T](t: T, e: Throwable) extends StreamStep[T]

  private case class StreamEnd[T](t: T) extends StreamStep[T]

  private case class StreamContinues[T](t: T) extends StreamStep[T]

}
