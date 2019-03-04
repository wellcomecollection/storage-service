package uk.ac.wellcome.platform.archive.bagunpacker.storage

import java.io.{BufferedInputStream, InputStream}

import grizzled.slf4j.Logging
import org.apache.commons.compress.archivers.{ArchiveEntry, ArchiveInputStream, ArchiveStreamFactory}
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.commons.io.input.CloseShieldInputStream

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object Unpacker extends Logging {

  def get[T](
    inputStream: InputStream
  )(init: T)(
    f: (T, InputStream, ArchiveEntry) => T
  )(implicit ec: ExecutionContext): Future[T] = Future {

    val archiveReader = new ArchiveReader[T](inputStream)

    @tailrec
    def foldStream(stream: InputStream)(t: T)(f: (T, InputStream, ArchiveEntry) => T): T = {
      archiveReader.accumulate(t, f) match {
        case Left(resultT)  => resultT
        case Right(resultT) => foldStream(stream)(resultT)(f)
      }
    }

    foldStream(inputStream)(init)(f)
  }

  private class ArchiveReader[T](inputStream: InputStream) {
    val unpack: Try[ArchiveInputStream] = for {
      compressorInputStream <- uncompress(
        new BufferedInputStream(inputStream)
      )

      archiveInputStream <- extract(
        new BufferedInputStream(compressorInputStream)
      )

    } yield archiveInputStream


    def accumulate(
              t: T,
              f: (T, InputStream, ArchiveEntry) => T,
            ): Either[T,T] = {

      unpack match {
        case Failure(t) => throw t
        case Success(archiveInputStream: ArchiveInputStream) => {
          archiveInputStream.getNextEntry match {
            case null => {
              archiveInputStream.close()
              inputStream.close()

              Left(t)
            }

            case entry: ArchiveEntry => {
              // Some clients might (AWS S3 SDK does!)
              // dispose of our InputStream,
              // we want to stop it as the Unpacker
              // object requires it to remain open
              // to retrieve the next entry.

              val closeShieldInputStream =
                new CloseShieldInputStream(archiveInputStream)

              Right(
                f(t, closeShieldInputStream, entry)
              )
            }
          }
        }
      }
    }

    def next(
              out: (InputStream, ArchiveEntry) => T,
            ): Option[T] = {

      unpack match {
        case Failure(t) => throw t
        case Success(archiveInputStream: ArchiveInputStream) => {
          archiveInputStream.getNextEntry match {
            case null => {
              archiveInputStream.close()
              inputStream.close()

              None
            }

            case entry: ArchiveEntry => {
              // Some clients might (AWS S3 SDK does!)
              // dispose of our InputStream,
              // we want to stop it as the Unpacker
              // object requires it to remain open
              // to retrieve the next entry.

              val closeShieldInputStream =
                new CloseShieldInputStream(archiveInputStream)

              Some(
                out(closeShieldInputStream, entry)
              )
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
  }
}
