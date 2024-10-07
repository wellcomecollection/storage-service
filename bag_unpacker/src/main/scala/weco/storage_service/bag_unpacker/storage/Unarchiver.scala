package weco.storage_service.bag_unpacker.storage

import java.io.{BufferedInputStream, InputStream}
import org.apache.commons.compress.archivers.{
  ArchiveEntry,
  ArchiveException,
  ArchiveInputStream,
  ArchiveStreamFactory
}
import org.apache.commons.compress.compressors.{
  CompressorException,
  CompressorInputStream,
  CompressorStreamFactory
}
import org.apache.commons.io.input.CloseShieldInputStream

import scala.util.{Failure, Success, Try}

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
object Unarchiver {
  def open(
    inputStream: InputStream
  ): Either[UnarchiverError, Iterator[(ArchiveEntry, InputStream)]] =
    for {
      uncompressedStream <- uncompress(inputStream)
      archiveInputStream <- extract(uncompressedStream)
      iterator = createIterator(archiveInputStream)
    } yield iterator

  private def createIterator(
    archiveInputStream: ArchiveInputStream[ArchiveEntry]
  ): Iterator[(ArchiveEntry, InputStream)] =
    new Iterator[(ArchiveEntry, InputStream)] {
      private var latest: ArchiveEntry = _
      private var seen: Set[ArchiveEntry] = Set.empty

      override def hasNext: Boolean = {
        latest = archiveInputStream.getNextEntry

        // The unpacker can potentially lose data if an archive contains duplicate files.
        // It's legal for a tar archive to contain two different files under the same name.
        // There's nothing sensible to do in this case (which should we pick?), so throw an
        // exception rather than unpacking any further.
        if (seen.contains(latest)) {
          throw new DuplicateArchiveEntryException(latest)
        }

        seen = seen | Set(latest)
        latest != null
      }

      override def next(): (ArchiveEntry, InputStream) =
        (latest, CloseShieldInputStream.wrap(archiveInputStream))
    }

  private def uncompress(
    compressedStream: InputStream
  ): Either[UnarchiverError, CompressorInputStream] =
    Try {
      // We have to wrap in a BufferedInputStream because this method
      // only takes InputStreams that support the `mark()` method.
      new CompressorStreamFactory()
        .createCompressorInputStream(new BufferedInputStream(compressedStream))
    } match {
      case Success(stream)                   => Right(stream)
      case Failure(err: CompressorException) => Left(CompressorError(err))
      case Failure(err)                      => Left(UnexpectedUnarchiverError(err))
    }
  private def extract(
    inputStream: InputStream
  ): Either[UnarchiverError, ArchiveInputStream[ArchiveEntry]] =
    Try {
      // We have to wrap in a BufferedInputStream because this method
      // only takes InputStreams that support the `mark()` method.
      new ArchiveStreamFactory()
        .createArchiveInputStream(new BufferedInputStream(inputStream))
        .asInstanceOf[ArchiveInputStream[ArchiveEntry]]
    } match {
      case Success(stream)                => Right(stream)
      case Failure(err: ArchiveException) => Left(ArchiveFormatError(err))
      case Failure(err)                   => Left(UnexpectedUnarchiverError(err))
    }
}

class DuplicateArchiveEntryException(val entry: ArchiveEntry)
    extends RuntimeException(
      s"Duplicate entries detected in archive: ${entry.getName}"
    )
