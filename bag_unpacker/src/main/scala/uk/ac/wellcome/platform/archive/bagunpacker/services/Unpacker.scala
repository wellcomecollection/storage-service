package uk.ac.wellcome.platform.archive.bagunpacker.services

import java.io.InputStream
import java.nio.file.Paths
import java.time.Instant

import org.apache.commons.compress.archivers.ArchiveEntry
import org.apache.commons.io.input.CloseShieldInputStream
import uk.ac.wellcome.platform.archive.bagunpacker.exceptions.ArchiveLocationException
import uk.ac.wellcome.platform.archive.bagunpacker.models.UnpackSummary
import uk.ac.wellcome.platform.archive.bagunpacker.storage.BetterArchive
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.storage.models.{IngestFailed, IngestStepResult, IngestStepSucceeded}
import uk.ac.wellcome.storage.{DoesNotExistError, ObjectLocation}
import uk.ac.wellcome.storage.store.Readable

import scala.util.{Failure, Success, Try}

case class Unpacker[IS <: InputStream](
  downloader: Readable[ObjectLocation, IS],
  s3Uploader: S3Uploader) {

  def unpack(
    ingestId: IngestID,
    srcLocation: ObjectLocation,
    dstLocation: ObjectLocation
  ): Try[IngestStepResult[UnpackSummary]] = {

    val unpackSummary =
      UnpackSummary(
        ingestId,
        srcLocation,
        dstLocation,
        startTime = Instant.now)

    val result = for {
      srcStream <- getSrcStream(srcLocation)
      unpackSummary <- unpack(unpackSummary, srcStream, dstLocation)
    } yield unpackSummary

    result match {
      case Success(summary) =>
        Success(IngestStepSucceeded(summary))
      case Failure(archiveLocationException: ArchiveLocationException) =>
        Success(
          IngestFailed(
            unpackSummary,
            archiveLocationException,
            Some(clientMessageFor(archiveLocationException))))
      case Failure(e) =>
        Success(IngestFailed(unpackSummary, e))
    }
  }

  private def clientMessageFor(exception: ArchiveLocationException) = {
    val archiveLocation = exception.getObjectLocation

    s"$archiveLocation could not be downloaded"
  }

  private def unpack(unpackSummary: UnpackSummary,
                     srcStream: InputStream,
                     dstLocation: ObjectLocation): Try[UnpackSummary] =
    BetterArchive.unpack(srcStream).map { iterator =>
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

  private def getSrcStream(
    srcLocation: ObjectLocation): Try[InputStream] =
    downloader.get(srcLocation) match {
      case Right(inputStream) => Success(inputStream.identifiedT)
      case Left(_: DoesNotExistError) =>
        Failure(
          new ArchiveLocationException(
            objectLocation = srcLocation,
            message =
              s"Error getting input stream for s3://$srcLocation: No such object"
          )
        )
      case Left(err) =>
        Failure(new ArchiveLocationException(
          objectLocation = srcLocation,
          message =
            s"Error getting input stream for s3://$srcLocation: $err"))
    }

  private def putObject(
    inputStream: InputStream,
    archiveEntry: ArchiveEntry,
    destination: ObjectLocation
  ): Long = {
    val uploadLocation = destination.copy(
      path = normalizeKey(destination.path, archiveEntry.getName)
    )

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
    s3Uploader.putObject(
      inputStream = new CloseShieldInputStream(inputStream),
      streamLength = archiveEntrySize,
      uploadLocation = uploadLocation
    )

    archiveEntrySize
  }

  private def normalizeKey(prefix: String, key: String) =
    Paths
      .get(prefix, key)
      .normalize()
      .toString
}
