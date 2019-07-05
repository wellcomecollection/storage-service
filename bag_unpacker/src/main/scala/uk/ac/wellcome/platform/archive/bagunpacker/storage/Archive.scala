package uk.ac.wellcome.platform.archive.bagunpacker.storage

import java.io.{BufferedInputStream, InputStream}

import org.apache.commons.compress.archivers.{ArchiveEntry, ArchiveInputStream, ArchiveStreamFactory}
import org.apache.commons.compress.compressors.{CompressorInputStream, CompressorStreamFactory}

import scala.util.Try

object Archive {
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
