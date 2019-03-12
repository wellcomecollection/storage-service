package uk.ac.wellcome.platform.archive.bagunpacker.services

import java.io.InputStream
import java.nio.file.Paths
import java.time.Instant

import com.amazonaws.services.s3.AmazonS3
import org.apache.commons.compress.archivers.ArchiveEntry
import uk.ac.wellcome.platform.archive.bagunpacker.models.UnpackSummary
import uk.ac.wellcome.platform.archive.bagunpacker.storage.Archive
import uk.ac.wellcome.platform.archive.common.operation.services.{OperationFailure, OperationResult}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.platform.archive.common.ConvertibleToInputStream._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class Unpacker(implicit s3Client: AmazonS3, ec: ExecutionContext) {

  private val s3Uploader = new S3Uploader()

  def unpack(
    srcLocation: ObjectLocation,
    dstLocation: ObjectLocation
  ): Future[OperationResult[UnpackSummary]] = {

    val unpackSummary =
      UnpackSummary(startTime = Instant.now)

    val futureSummary = for {
      packageInputStream <- srcLocation.toInputStream

      result <- Archive
        .unpack[UnpackSummary](packageInputStream)(unpackSummary) {
          (summary: UnpackSummary,
           inputStream: InputStream,
           archiveEntry: ArchiveEntry) =>
            if (!archiveEntry.isDirectory) {

              val archiveEntrySize = putObject(
                inputStream,
                archiveEntry,
                dstLocation
              )

              summary.copy(
                fileCount = summary.fileCount + 1,
                bytesUnpacked = summary.bytesUnpacked + archiveEntrySize
              )

            } else {
              summary
            }
        }
    } yield result.withSummary(summary = result.summary.complete)

    futureSummary.transform {
      case Success(summary) => Success(summary)
      case Failure(e) =>
        Success(
          OperationFailure(unpackSummary.complete, e)
        )
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
