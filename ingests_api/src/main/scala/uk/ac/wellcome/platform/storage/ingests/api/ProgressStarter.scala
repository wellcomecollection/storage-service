package uk.ac.wellcome.platform.storage.ingests.api

import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.SNSWriter
import uk.ac.wellcome.platform.archive.common.models.{
  IngestBagRequest,
  StorageSpace,
  UnpackRequest
}
import uk.ac.wellcome.platform.archive.common.progress.models.Progress
import uk.ac.wellcome.platform.archive.common.progress.monitor.ProgressTracker

import scala.concurrent.{ExecutionContext, Future}

class ProgressStarter(
  progressTracker: ProgressTracker,
  archivistSnsWriter: SNSWriter,
  unpackerSnsWriter: SNSWriter)(implicit ec: ExecutionContext) {
  def initialise(progress: Progress): Future[Progress] =
    for {
      progress <- progressTracker.initialise(progress)
      _ <- archivistSnsWriter.writeMessage(
        toIngestRequest(progress),
        "progress-http-request-created"
      )
      _ <- unpackerSnsWriter.writeMessage(
        toUnpackRequest(progress),
        "progress-http-request-created"
      )
    } yield progress

  private def toIngestRequest(
    progress: Progress
  ) = IngestBagRequest(
    id = progress.id,
    zippedBagLocation = progress.sourceLocation.location,
    archiveCompleteCallbackUrl = progress.callback.map { _.uri },
    storageSpace = StorageSpace(progress.space.underlying)
  )

  private def toUnpackRequest(
    progress: Progress
  ): UnpackRequest = {

    UnpackRequest(
      requestId = progress.id,
      sourceLocation = progress.sourceLocation.location,
      storageSpace = StorageSpace(progress.space.underlying)
    )
  }
}
