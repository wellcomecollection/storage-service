package uk.ac.wellcome.platform.archive.common.operation

import java.util.UUID

import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.{PublishAttempt, SNSWriter}
import uk.ac.wellcome.platform.archive.common.models.bagit.BagId
import uk.ac.wellcome.platform.archive.common.progress.models.{Progress, ProgressEvent, ProgressStatusUpdate, ProgressUpdate}

import scala.concurrent.Future

class IngestNotifier(
  operationName: String,
  snsWriter: SNSWriter
) {

  def send[R](
    requestId: UUID,
    result: OperationResult[R],
    bagId: Option[BagId] = None
  ): Future[PublishAttempt] = {
    val update = result match {
      case OperationCompleted(_) =>
        ProgressStatusUpdate(
          id = requestId,
          status = Progress.Completed,
          affectedBag = bagId,
          events = List(
            ProgressEvent(
              s"${operationName.capitalize} succeeded (completed)"
            )
          )
        )

      case OperationSuccess(_) =>
        ProgressUpdate.event(
          id = requestId,
          description = s"${operationName.capitalize} succeeded"
        )

      case OperationFailure(_, _) =>
        ProgressStatusUpdate(
          id = requestId,
          status = Progress.Failed,
          affectedBag = bagId,
          events = List(
            ProgressEvent(
              s"${operationName.capitalize} failed"
            )
          )
        )
    }

    snsWriter.writeMessage[ProgressUpdate](
      update,
      subject = s"Sent by ${this.getClass.getSimpleName}"
    )
  }
}
