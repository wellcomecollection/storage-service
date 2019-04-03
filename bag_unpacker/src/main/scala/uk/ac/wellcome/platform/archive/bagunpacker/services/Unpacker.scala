package uk.ac.wellcome.platform.archive.bagunpacker.services

import java.io.InputStream
import java.nio.file.Paths
import java.time.Instant

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.AmazonS3Exception
import org.apache.commons.compress.archivers.ArchiveEntry
import uk.ac.wellcome.platform.archive.bagunpacker.models.UnpackSummary
import uk.ac.wellcome.platform.archive.bagunpacker.storage.Archive
import uk.ac.wellcome.platform.archive.common.ConvertibleToInputStream._
import uk.ac.wellcome.platform.archive.common.exception.PutObjectLocationException
import uk.ac.wellcome.platform.archive.common.operation.models.{WorkFailed, WorkResult, WorkSucceeded}
import uk.ac.wellcome.storage.ObjectLocation

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

case class Unpacker(s3Uploader: S3Uploader)(implicit s3Client: AmazonS3,
                                            ec: ExecutionContext) {

  def unpack(
    requestId: String,
    srcLocation: ObjectLocation,
    dstLocation: ObjectLocation): Future[WorkResult[UnpackSummary]] = {

    val unpackSummary =
      UnpackSummary(
        requestId,
        srcLocation,
        dstLocation,
        startTime = Instant.now)

    val futureSummary = for {
      packageInputStream <- srcLocation.toInputStream

      unpackSummary <- Archive
        .unpack[UnpackSummary](packageInputStream)(unpackSummary) {
          (summary: UnpackSummary,
           inputStream: InputStream,
           archiveEntry: ArchiveEntry) =>
            if (!archiveEntry.isDirectory) {
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
                case ae : AmazonS3Exception =>
                  throw new PutObjectLocationException(dstLocation,"upload failed", ae)
              }

            } else {
              summary
            }
        }
    } yield unpackSummary

    futureSummary.transform {
      case Success(summary) =>
          Success(WorkSucceeded(summary))
      case Failure(e) =>
          Success(WorkFailed(unpackSummary, e))
    }
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
