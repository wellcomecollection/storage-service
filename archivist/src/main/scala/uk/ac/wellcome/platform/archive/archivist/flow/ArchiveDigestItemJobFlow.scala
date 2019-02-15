package uk.ac.wellcome.platform.archive.archivist.flow

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.archivist.models.DigestItemJob
import uk.ac.wellcome.platform.archive.common.flows.{
  FoldEitherFlow,
  OnErrorFlow
}
import uk.ac.wellcome.platform.archive.common.models.error.ArchiveError

object ArchiveDigestItemJobFlow extends Logging {
  def apply(parallelism: Int)(
    implicit s3Client: AmazonS3
  ): Flow[DigestItemJob,
          Either[ArchiveError[DigestItemJob], DigestItemJob],
          NotUsed] = {
    Flow[DigestItemJob]
      .log("uploading and verifying")
      .via(UploadDigestItemFlow(parallelism))
      .via(
        FoldEitherFlow[
          ArchiveError[DigestItemJob],
          DigestItemJob,
          Either[ArchiveError[DigestItemJob], DigestItemJob]](
          ifLeft = OnErrorFlow())(
          ifRight = DownloadAndVerifyDigestItemFlow(parallelism)))
      .log("download verified")
  }
}
