package uk.ac.wellcome.platform.archive.bagunpacker.services

import java.io.InputStream
import java.nio.file.Paths
import java.time.Instant

import com.amazonaws.services.s3.AmazonS3
import org.apache.commons.compress.archivers.ArchiveEntry
import uk.ac.wellcome.platform.archive.bagunpacker.exceptions.{ArchiveLocationException, UnpackerArchiveEntryUploadException}
import uk.ac.wellcome.platform.archive.bagunpacker.models.UnpackSummary
import uk.ac.wellcome.platform.archive.bagunpacker.storage.Archive
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.storage.models.{IngestFailed, IngestStepResult, IngestStepSucceeded}
import uk.ac.wellcome.platform.archive.common.storage.services.S3StreamableInstances._
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.store.Readable

import scala.util.{Failure, Success, Try}

case class Unpacker(
  downloader: Readable[ObjectLocation, _ <: InputStream],
  s3Uploader: S3Uploader)(implicit s3Client: AmazonS3) {

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
      archiveInputStream <- archiveDownloadStream(srcLocation)
      unpackSummary <- unpack(unpackSummary, archiveInputStream, dstLocation)
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
                     packageInputStream: InputStream,
                     dstLocation: ObjectLocation): Try[UnpackSummary] =
    Archive
      .unpack[UnpackSummary](packageInputStream)(unpackSummary) {
        (summary: UnpackSummary,
         inputStream: InputStream,
         archiveEntry: ArchiveEntry) =>
          if (!archiveEntry.isDirectory) {
            putArchiveEntry(dstLocation, summary, inputStream, archiveEntry)
          } else {
            summary
          }
      }

  private def archiveDownloadStream(
    srcLocation: ObjectLocation): Try[InputStream] =
    srcLocation.toInputStream match {
      case Right(Some(is)) => Success(is)
      case Right(None) =>
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
            s"Error getting input stream for s3://$srcLocation: ${err.getMessage}"))
    }

  private def putArchiveEntry(dstLocation: ObjectLocation,
                              summary: UnpackSummary,
                              inputStream: InputStream,
                              archiveEntry: ArchiveEntry) =
    try {
      val archiveEntrySize = putObject(
        inputStream,
        archiveEntry,
        dstLocation
      )
      summary.copy(
        fileCount = summary.fileCount + 1,
        bytesUnpacked = summary.bytesUnpacked + archiveEntrySize
      )
    } catch {
      case err: Throwable =>
        throw new UnpackerArchiveEntryUploadException(
          dstLocation,
          s"upload failed for ${archiveEntry.getName}",
          err)
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

    s3Uploader.putObject(
      inputStream = inputStream,
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
