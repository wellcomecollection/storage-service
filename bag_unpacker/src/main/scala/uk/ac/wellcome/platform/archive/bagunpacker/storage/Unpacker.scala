package uk.ac.wellcome.platform.archive.bagunpacker.storage

import java.io.{BufferedInputStream, InputStream}

import grizzled.slf4j.Logging
import org.apache.commons.compress.archivers.{ArchiveEntry, ArchiveInputStream, ArchiveStreamFactory}
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.commons.io.input.CloseShieldInputStream
import uk.ac.wellcome.platform.archive.bagunpacker.config.models.{OperationFailure, OperationResult, OperationSuccess}

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object Unpacker extends Logging {

  def unpack[T](
    inputStream: InputStream
  )(init: T)(
    f: (T, InputStream, ArchiveEntry) => T
  )(implicit ec: ExecutionContext):
  Future[OperationResult[T]] = Future {

    val archiveReader =
      new ArchiveReader[T](inputStream)

    @tailrec
    def foldStream(stream: InputStream)(t: T)(
      f: (T, InputStream, ArchiveEntry) => T
    ): OperationResult[T] = {

      archiveReader.accumulate(t, f) match {
        case StreamEnd(resultT) =>
          OperationSuccess(resultT)
        case StreamContinues(resultT) =>
          foldStream(stream)(resultT)(f)
        case StreamError(resultT, e) =>
          OperationFailure(resultT, e)
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
        case Success(
          archiveInputStream: ArchiveInputStream
        ) => {
          archiveInputStream.getNextEntry match {
            case null => {
              Try {
                archiveInputStream.close()
                inputStream.close()
              } match {
                case Success(_) => StreamEnd(t)
                case Failure(e) => StreamError(t,e)
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
                  new CloseShieldInputStream(archiveInputStream)

                f(t, closeShieldInputStream, entry)
              } match {
                case Success(r) => StreamContinues(r)
                case Failure(e) => StreamError(t,e)
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
