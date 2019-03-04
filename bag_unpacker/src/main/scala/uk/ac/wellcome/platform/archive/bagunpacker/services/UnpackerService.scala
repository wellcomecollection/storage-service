package uk.ac.wellcome.platform.archive.bagunpacker.services

import java.io.InputStream
import java.nio.file.Paths
import java.time.Instant

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest}
import org.apache.commons.compress.archivers.ArchiveEntry
import uk.ac.wellcome.platform.archive.bagunpacker.models.UnpackSummary
import uk.ac.wellcome.platform.archive.bagunpacker.storage.Unpacker
import uk.ac.wellcome.platform.archive.common.ConvertibleToInputStream._
import uk.ac.wellcome.storage.ObjectLocation

import scala.concurrent.ExecutionContext


class UnpackerService(implicit s3Client: AmazonS3, ec: ExecutionContext) {

  def unpack(
              srcLocation: ObjectLocation,
              dstLocation: ObjectLocation
            ) = {
    val unpackSummary = UnpackSummary(startTime = Instant.now)

    (for {
      packageInputStream <- srcLocation.toInputStream
      result <- Unpacker.get(packageInputStream)(unpackSummary) {
        (summary: UnpackSummary,
         inputStream: InputStream,
         archiveEntry: ArchiveEntry) =>

          val metadata = new ObjectMetadata()
          val archiveEntrySize = archiveEntry.getSize

          if(archiveEntrySize == ArchiveEntry.SIZE_UNKNOWN) {
            throw new RuntimeException(
              s"Unknown entry size for ${archiveEntry.getName}!"
            )
          }

          metadata.setContentLength(archiveEntry.getSize)

          val request = new PutObjectRequest(dstLocation.namespace,
            Paths.get(
              dstLocation.key,
              archiveEntry.getName
            ).toString,
            inputStream,
            metadata
          )

          s3Client.putObject(request)

          summary.copy(
            fileCount = summary.fileCount + 1,
            bytesUnpacked = summary.bytesUnpacked + archiveEntrySize
          )
      }
    } yield result).map(_.complete)

  }
}


