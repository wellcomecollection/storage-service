package uk.ac.wellcome.platform.archive.archivist.flow

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.archivist.builders.UploadLocationBuilder
import uk.ac.wellcome.platform.archive.archivist.models._
import uk.ac.wellcome.platform.archive.archivist.models.errors.{
  ArchiveItemJobError,
  ArchiveJobError
}
import uk.ac.wellcome.platform.archive.common.models.error.ArchiveError

/** This flow extracts a tag manifest from a ZIP file, and uploads it to S3
  *
  * It emits the original archive job.
  *
  * It returns an error if:
  *   - There's a problem getting the item from the ZIP file
  *   - The upload to S3 fails
  *
  */
object ArchiveTagManifestFlow extends Logging {
  def apply(parallelism: Int)(implicit s3Client: AmazonS3)
    : Flow[ArchiveJob, Either[ArchiveError[ArchiveJob], ArchiveJob], NotUsed] =
    Flow[ArchiveJob]
      .log("archiving tag manifest")
      .map(createTagManifestItemJob)
      .via(UploadItemFlow(parallelism))
      .via(
        FoldEitherFlow[
          ArchiveError[ArchiveItemJob],
          (ArchiveItemJob, String),
          Either[ArchiveError[ArchiveJob], ArchiveJob]](
          ifLeft = Flow[ArchiveError[ArchiveItemJob]].map { error =>
            Left(ArchiveItemJobError(error.t.archiveJob, List(error)))
          })(
          ifRight = Flow[(ArchiveItemJob, String)]
            .map(context => createDigestItemJob _ tupled context)
            .via(DownloadAndVerifyDigestItemFlow(parallelism))
            .via(extractArchiveJobFlow)
        )
      )

  private def createTagManifestItemJob(
    archiveJob: ArchiveJob): TagManifestItemJob =
    TagManifestItemJob(
      archiveJob = archiveJob,
      zipEntryPointer = ZipEntryPointer(
        zipFile = archiveJob.zipFile,
        zipPath = archiveJob.tagManifestLocation.underlying
      ),
      uploadLocation = UploadLocationBuilder.create(
        bagUploadLocation = archiveJob.bagUploadLocation,
        itemPathInZip = archiveJob.tagManifestLocation.underlying,
        maybeBagRootPathInZip = archiveJob.maybeBagRootPathInZip
      )
    )

  private def createDigestItemJob(archiveItemJob: ArchiveItemJob,
                                  digest: String): DigestItemJob =
    DigestItemJob(
      archiveJob = archiveItemJob.archiveJob,
      zipEntryPointer = archiveItemJob.zipEntryPointer,
      uploadLocation = archiveItemJob.uploadLocation,
      digest = digest
    )

  private def extractArchiveJobFlow = {
    FoldEitherFlow[
      ArchiveError[DigestItemJob],
      DigestItemJob,
      Either[ArchiveError[ArchiveJob], ArchiveJob]](
      ifLeft = Flow[ArchiveError[DigestItemJob]].map { error =>
        Left(ArchiveJobError(error.t.archiveJob, List(error)))
      })(ifRight = Flow[DigestItemJob].map { archiveDigestItemJob =>
      Right(archiveDigestItemJob.archiveJob)
    })
  }
}
