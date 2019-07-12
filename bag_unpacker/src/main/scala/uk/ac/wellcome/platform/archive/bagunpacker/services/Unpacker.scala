package uk.ac.wellcome.platform.archive.bagunpacker.services

import java.io.InputStream
import java.time.Instant

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
import uk.ac.wellcome.storage.{
  DoesNotExistError,
  ObjectLocation,
  ObjectLocationPrefix,
  StorageError
}

import scala.util.{Failure, Success, Try}

trait Unpacker {
  // The unpacker asks for separate get/put methods rather than a Store
  // because it might be unpacking/uploading to different providers.
  //
  // e.g. we might unpack a package from an S3 bucket, then upload it to Azure.
  //
  def get(location: ObjectLocation): Either[StorageError, InputStream]
  def put(location: ObjectLocation)(
    inputStream: InputStreamWithLength): Either[StorageError, Unit]

  def unpack(
    ingestId: IngestID,
    srcLocation: ObjectLocation,
    dstLocation: ObjectLocationPrefix
  ): Try[IngestStepResult[UnpackSummary]] = {
    val unpackSummary =
      UnpackSummary(ingestId, srcLocation, dstLocation, startTime = Instant.now)

    val result = for {
      srcStream <- get(srcLocation).left.map { storageError =>
        UnpackerStorageError(storageError)
      }

      unpackSummary <- unpack(unpackSummary, srcStream, dstLocation)
    } yield unpackSummary

    result match {
      case Right(summary) =>
        Success(IngestStepSucceeded(summary))

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

  protected def buildMessageFor(srcLocation: ObjectLocation,
                                error: UnpackerError): Option[String] =
    error match {
      case UnpackerStorageError(_: DoesNotExistError) =>
        Some(s"There is no archive at $srcLocation")

      case _ => None
    }

  private def unpack(
    unpackSummary: UnpackSummary,
    srcStream: InputStream,
    dstLocation: ObjectLocationPrefix): Either[UnpackerError, UnpackSummary] =
    Unarchiver.open(srcStream) match {
      case Left(unarchiverError) =>
        Left(UnpackerUnarchiverError(unarchiverError))

      case Right(iterator) =>
        var totalFiles = 0
        var totalBytes = 0

        Try {
          iterator
            .filterNot { case (archiveEntry, _) => archiveEntry.isDirectory }
            .foreach {
              case (archiveEntry, entryStream) =>
                val uploadedBytes = putObject(
                  inputStream = entryStream,
                  archiveEntry = archiveEntry,
                  destination = dstLocation
                )

                totalFiles += 1
                totalBytes += uploadedBytes.toInt
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
    destination: ObjectLocationPrefix
  ): Long = {
    val uploadLocation = destination.asLocation(archiveEntry.getName)

    val archiveEntrySize = archiveEntry.getSize

    if (archiveEntrySize == ArchiveEntry.SIZE_UNKNOWN) {
      throw new RuntimeException(
        s"Unknown entry size for ${archiveEntry.getName}!"
      )
    }

    put(uploadLocation)(
      new InputStreamWithLength(inputStream, length = archiveEntrySize)) match {
      case Right(_)           => ()
      case Left(storageError) => throw storageError.e
    }

    archiveEntrySize
  }
}
