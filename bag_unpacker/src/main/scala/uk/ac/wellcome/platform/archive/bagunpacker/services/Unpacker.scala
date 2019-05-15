package uk.ac.wellcome.platform.archive.bagunpacker.services

import java.io.InputStream
import java.nio.file.Paths
import java.time.Instant

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.AmazonS3Exception
import org.apache.commons.compress.archivers.ArchiveEntry
import uk.ac.wellcome.platform.archive.bagunpacker.exceptions.{
  ArchiveLocationException,
  UnpackerArchiveEntryUploadException
}
import uk.ac.wellcome.platform.archive.bagunpacker.models.UnpackSummary
import uk.ac.wellcome.platform.archive.bagunpacker.storage.Archive
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestFailed,
  IngestStepResult,
  IngestStepSucceeded
}
import uk.ac.wellcome.storage.ObjectLocation

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

case class Unpacker(s3Uploader: S3Uploader)(implicit s3Client: AmazonS3,
                                            ec: ExecutionContext) {

  import uk.ac.wellcome.platform.archive.common.storage.services.S3StreamableInstances._

  def unpack(
    requestId: String,
    srcLocation: ObjectLocation,
    dstLocation: ObjectLocation): Future[IngestStepResult[UnpackSummary]] = {

    val unpackSummary =
      UnpackSummary(
        requestId,
        srcLocation,
        dstLocation,
        startTime = Instant.now)

    val futureSummary = for {
      archiveInputStream <- archiveDownloadStream(srcLocation)
      unpackSummary <- unpack(unpackSummary, archiveInputStream, dstLocation)
    } yield unpackSummary

    futureSummary.transform {
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
    val cause = exception.getCause.asInstanceOf[AmazonS3Exception]
    val archiveLocation = exception.getObjectLocation
    cause.getStatusCode match {
      case 403 => s"access to $archiveLocation is denied"
      case 404 => s"$archiveLocation does not exist"
      case _   => s"$archiveLocation could not be downloaded"
    }
  }

  private def unpack(unpackSummary: UnpackSummary,
                     packageInputStream: InputStream,
                     dstLocation: ObjectLocation) = {
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
  }

  private def archiveDownloadStream(srcLocation: ObjectLocation) = {
    Future
      .fromTry(srcLocation.toInputStream)
      .recoverWith {
        case ae: AmazonS3Exception =>
          Future.failed(
            new ArchiveLocationException(
              objectLocation = srcLocation,
              message =
                s"Error getting input stream for s3://$srcLocation: ${ae.getMessage}",
              ae))
      }
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
      case ae: AmazonS3Exception =>
        throw new UnpackerArchiveEntryUploadException(
          dstLocation,
          s"upload failed for ${archiveEntry.getName}",
          ae)
    }

  private def putObject(
    inputStream: InputStream,
    archiveEntry: ArchiveEntry,
    destination: ObjectLocation
  ): Long = {
    val uploadLocation = destination.copy(
      key = normalizeKey(destination.key, archiveEntry.getName)
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
