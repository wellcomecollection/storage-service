package uk.ac.wellcome.platform.archive.bagunpacker.storage

import java.io.{BufferedInputStream, InputStream}

import grizzled.slf4j.Logging
import org.apache.commons.compress.archivers.{ArchiveEntry, ArchiveInputStream, ArchiveStreamFactory}
import org.apache.commons.compress.compressors.CompressorStreamFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object Unpack extends Logging {
//
//  implicit private class PrivateArchiveInputStream(archiveInputStream: ArchiveInputStream) {
//    def close() = {
//      println(
//        "Stream will not be closed, please call reallyClose()!"
//      )
//    }
//
//    def reallyClose() = {
//      archiveInputStream.close()
//    }
//  }

  def get[T](
    inputStream: InputStream
  )(
    out: (InputStream, ArchiveEntry) => T
  )(implicit ec: ExecutionContext): Future[List[T]] = Future {

    val archiveReader = new ArchiveReader[T](inputStream)

    def entries(stream: InputStream): Stream[T] = {
      archiveReader.next(out) match {
        case None        => Stream.empty
        case Some(entry) => entry #:: entries(stream)
      }
    }

    entries(inputStream)
      .toList
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


    def next(
              out: (InputStream, ArchiveEntry) => T
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
              Some(
                out(archiveInputStream, entry)
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
