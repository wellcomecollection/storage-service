package uk.ac.wellcome.platform.archive.bagunpacker.services

import java.io.InputStream
import java.time.Instant

import grizzled.slf4j.Logging
import org.apache.commons.compress.archivers.ArchiveEntry
import uk.ac.wellcome.platform.archive.bagunpacker.models.UnpackSummary
import uk.ac.wellcome.platform.archive.bagunpacker.storage.Unarchiver
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestFailed,
  IngestStepResult,
  IngestStepSucceeded
}
import uk.ac.wellcome.storage.streaming.InputStreamWithLength
import uk.ac.wellcome.storage._

import scala.util.{Failure, Success, Try}

trait Unpacker[
  SrcLocation <: Location,
  DstLocation <: Location,
  DstPrefix <: Prefix[DstLocation]
] extends Logging {

  // The unpacker asks for separate get/put methods rather than a Store
  // because it might be unpacking/uploading to different providers.
  //
  // e.g. we might unpack a package from an S3 bucket, then upload it to Azure.
  //
  def get(location: SrcLocation): Either[StorageError, InputStream]
  def put(location: DstLocation)(
    inputStream: InputStreamWithLength
  ): Either[StorageError, Unit]

  def unpack(
    ingestId: IngestID,
    srcLocation: SrcLocation,
    dstPrefix: DstPrefix
  ): Try[IngestStepResult[UnpackSummary[SrcLocation, DstPrefix]]] = {
    val unpackSummary =
      UnpackSummary(
        ingestId = ingestId,
        srcLocation = srcLocation,
        dstPrefix = dstPrefix,
        startTime = Instant.now
      )

    val result = for {
      srcStream <- get(srcLocation).left.map { storageError =>
        UnpackerStorageError(storageError)
      }

      unpackSummary <- unpack(unpackSummary, srcStream, dstPrefix)
    } yield unpackSummary

    result match {
      case Right(summary) =>
        Success(
          IngestStepSucceeded(
            summary,
            maybeUserFacingMessage = Some(UnpackerMessage.create(summary))
          )
        )

      case Left(unpackerError) =>
        Success(
          IngestFailed(
            unpackSummary,
            e = unpackerError.e,
            maybeUserFacingMessage = buildMessageFor(
              srcLocation,
              error = unpackerError
            )
          )
        )
    }
  }

  protected def buildMessageFor(
    srcLocation: SrcLocation,
    error: UnpackerError
  ): Option[String] =
    error match {
      case UnpackerStorageError(_: DoesNotExistError) =>
        Some(s"There is no archive at $srcLocation")

      case UnpackerUnarchiverError(_) =>
        Some(
          s"Error trying to unpack the archive at $srcLocation - is it the correct format?"
        )

      case _ => None
    }

  private def unpack(
    unpackSummary: UnpackSummary[SrcLocation, DstPrefix],
    srcStream: InputStream,
    dstPrefix: DstPrefix
  ): Either[UnpackerError, UnpackSummary[SrcLocation, DstPrefix]] =
    Unarchiver.open(srcStream) match {
      case Left(unarchiverError) =>
        Left(UnpackerUnarchiverError(unarchiverError))

      case Right(iterator) =>
        // For large bags, the standard Int type can overflow and report a negative
        // number of bytes.  This is silly, so we ensure these are treated as Long.
        // See https://github.com/wellcometrust/platform/issues/3947
        var totalFiles: Long = 0
        var totalBytes: Long = 0

        Try {
          iterator
            .filterNot { case (archiveEntry, _) => archiveEntry.isDirectory }
            .foreach {
              case (archiveEntry, entryStream) =>
                debug(s"Processing archive entry ${archiveEntry.getName}")
                val uploadedBytes = putObject(
                  inputStream = entryStream,
                  archiveEntry = archiveEntry,
                  dstPrefix = dstPrefix
                )

                totalFiles += 1
                totalBytes += uploadedBytes
            }

          unpackSummary.copy(
            fileCount = totalFiles,
            bytesUnpacked = totalBytes
          )
        } match {
          case Success(result) => Right(result)
          case Failure(err: StorageError) =>
            Left(UnpackerStorageError(err))
          case Failure(err: Throwable) =>
            Left(UnpackerUnexpectedError(err))
        }
    }

  private def putObject(
    inputStream: InputStream,
    archiveEntry: ArchiveEntry,
    dstPrefix: DstPrefix
  ): Long = {

    // Sometimes the entries in a tar.gz archive are prefixed with ./, for example:
    //
    //      ./PBLBIO/bag-info.txt
    //
    // We don't want to include the leading `./` in the names we write to the
    // unpacked bags bucket, because they can cause issues in the S3 console and the
    // root finder.
    //
    // We do this normalisation manually rather than normalising the whole string,
    // so that we can spot any weirdness with overlapping entries.  e.g.
    //
    //      my_bag/data/cat.jpg
    //      my_bag/data/pictures/../cat.jpg
    //
    // could be two distinct entries in the tar.gz, but different objects.  We don't
    // want to unpack them to the same object; we want to notice and throw an error.
    //
    val name = archiveEntry.getName.stripPrefix("./")
    val uploadLocation = dstPrefix.asLocation(name)

    val archiveEntrySize = archiveEntry.getSize

    if (archiveEntrySize == ArchiveEntry.SIZE_UNKNOWN) {
      throw new RuntimeException(
        s"Unknown entry size for ${archiveEntry.getName}!"
      )
    }

    debug(
      s"Uploading archive entry ${archiveEntry.getName} to $uploadLocation"
    )

    put(uploadLocation)(
      new InputStreamWithLength(inputStream, length = archiveEntrySize)
    ) match {
      case Right(_)           => ()
      case Left(storageError) => throw storageError.e
    }

    archiveEntrySize
  }
}
