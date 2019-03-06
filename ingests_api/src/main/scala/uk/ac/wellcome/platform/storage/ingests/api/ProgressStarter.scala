package uk.ac.wellcome.platform.storage.ingests.api

import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.SNSWriter
import uk.ac.wellcome.platform.archive.common.models.{
  IngestBagRequest,
  StorageSpace,
  UnpackBagRequest
}
import uk.ac.wellcome.platform.archive.common.progress.models.Progress
import uk.ac.wellcome.platform.archive.common.progress.monitor.ProgressTracker

import scala.concurrent.{ExecutionContext, Future}

class ProgressStarter(
  progressTracker: ProgressTracker,
  unpackerSnsWriter: SNSWriter
)(implicit ec: ExecutionContext) {
  def initialise(progress: Progress): Future[Progress] =
    for {
      _ <- unpackerSnsWriter.writeMessage(
        toUnpackRequest(progress),
        "progress-http-request-created"
      )
    } yield progress

  private def toUnpackRequest(
    progress: Progress
  ): UnpackBagRequest = {

    UnpackBagRequest(
      requestId = progress.id,
      sourceLocation = progress.sourceLocation.location,
      storageSpace = StorageSpace(progress.space.underlying)
    )
  }
}
