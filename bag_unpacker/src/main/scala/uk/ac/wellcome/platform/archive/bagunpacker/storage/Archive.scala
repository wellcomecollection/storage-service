package uk.ac.wellcome.platform.archive.bagunpacker.storage

import java.io.{BufferedInputStream, InputStream}

import org.apache.commons.compress.archivers.{ArchiveEntry, ArchiveInputStream, ArchiveStreamFactory}
import org.apache.commons.compress.compressors.{CompressorInputStream, CompressorStreamFactory}
import org.apache.commons.io.input.CloseShieldInputStream

import scala.util.Try

/** You pass this class an inputStream that comes from the storage provider,
  * e.g. a FileInputStream or an S3InputStream.  The stream should be a tar.gz
  * or similar container + compression format.
  *
  * The iterator it produces has two pieces:
  *  1. ArchiveEntry -- metadata about the original file, e.g. name/size
  *  2. InputStream -- a stream that gives you the content for this file
  *
  * This InputStream is really just a view into the original stream.
  * If you advance one, the other advances.  They move together.
  *
  *       +------------------------------------------+ original stream
  *
  *       +-------+                                    file1
  *               +------------------+                 file2
  *                                  +---------+       file3
  *                                            +-----+ file4
  *
  * This is why we wrap the output in a close shield: if the caller closed
  * the individual stream, they'd close the underlying stream and we'd be
  * unable to read any more of the archive.
  *
  */
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
        (latest, new CloseShieldInputStream(archiveInputStream))
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
