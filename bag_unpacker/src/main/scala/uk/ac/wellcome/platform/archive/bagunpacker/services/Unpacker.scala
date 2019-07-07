package uk.ac.wellcome.platform.archive.bagunpacker.services

import java.io.InputStream
import java.time.Instant

import org.apache.commons.compress.archivers.ArchiveEntry
import uk.ac.wellcome.platform.archive.bagunpacker.models.UnpackSummary
import uk.ac.wellcome.platform.archive.bagunpacker.storage.{Unarchiver, UnarchiverError}
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.storage.models.{IngestFailed, IngestStepResult, IngestStepSucceeded}
import uk.ac.wellcome.storage.streaming.InputStreamWithLength
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix, StorageError}

import scala.util.{Success, Try}

trait Unpacker {
  // The unpacker asks for separate get/put methods rather than a Store
  // because it might be unpacking/uploading to different providers.
  //
  // e.g. we might unpack a package from an S3 bucket, then upload it to Azure.
  //
  def get(location: ObjectLocation): Either[StorageError, InputStream]
  def put(location: ObjectLocation)(inputStream: InputStreamWithLength): Either[StorageError, Unit]

  def unpack(
    ingestId: IngestID,
    srcLocation: ObjectLocation,
    dstLocation: ObjectLocationPrefix
  ): Try[IngestStepResult[UnpackSummary]] = {
    val unpackSummary =
      UnpackSummary(
        ingestId,
        srcLocation,
        dstLocation,
        startTime = Instant.now)

    val result = for {
      srcStream <- get(srcLocation)
        .left.map { storageError => UnpackerStorageError(storageError) }

      unpackSummary <- unpack(unpackSummary, srcStream, dstLocation)
        .left.map { unarchiverError => UnpackerUnarchiverError(unarchiverError) }
    } yield unpackSummary

    result match {
      case Right(summary) =>
        Success(IngestStepSucceeded(summary))

      case Left(UnpackerStorageError(storageError)) =>
        Success(
          IngestFailed(
            unpackSummary,
            e = storageError.e
          )
        )

      case Left(UnpackerUnarchiverError(unarchiverError)) =>
        Success(
          IngestFailed(
            unpackSummary,
            e = unarchiverError.e
          )
        )
    }
  }

  private def unpack(unpackSummary: UnpackSummary,
                     srcStream: InputStream,
                     dstLocation: ObjectLocationPrefix): Either[UnarchiverError, UnpackSummary] =
    Unarchiver.open(srcStream).map { iterator =>
      var totalFiles = 0
      var totalBytes = 0

      iterator
        .filterNot { case (archiveEntry, _) => archiveEntry.isDirectory }
        .foreach { case (archiveEntry, entryStream) =>
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

    // The S3 SDK will "helpfully" attempt to close the input stream when
    // it's finished uploading.  Because this is really a view into the underlying
    // stream coming from the original archive, we don't want to close it -- hold
    // it open.
    put(uploadLocation)(
      new InputStreamWithLength(inputStream, length = archiveEntrySize)) match {
      case Right(_) => ()
      case Left(storageError) => throw storageError.e
    }

    archiveEntrySize
  }
}
