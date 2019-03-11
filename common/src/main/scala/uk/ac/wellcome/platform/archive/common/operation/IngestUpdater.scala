package uk.ac.wellcome.platform.archive.common.operation

import java.util.UUID

import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.{PublishAttempt, SNSWriter}
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Ingest,
  IngestEvent,
  IngestStatusUpdate,
  IngestUpdate
}
import uk.ac.wellcome.platform.archive.common.ingests.operation.{
  OperationCompleted,
  OperationFailure,
  OperationResult,
  OperationSuccess
}
import uk.ac.wellcome.platform.archive.common.models.bagit.BagId

import scala.concurrent.Future

class IngestUpdater(
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
        IngestStatusUpdate(
          id = requestId,
          status = Ingest.Completed,
          affectedBag = bagId,
          events = List(
            IngestEvent(
              s"${operationName.capitalize} succeeded (completed)"
            )
          )
        )

      case OperationSuccess(_) =>
        IngestUpdate.event(
          id = requestId,
          description = s"${operationName.capitalize} succeeded"
        )

      case OperationFailure(_, _) =>
        IngestStatusUpdate(
          id = requestId,
          status = Ingest.Failed,
          affectedBag = bagId,
          events = List(
            IngestEvent(
              s"${operationName.capitalize} failed"
            )
          )
        )
    }

    snsWriter.writeMessage[IngestUpdate](
      update,
      subject = s"Sent by ${this.getClass.getSimpleName}"
    )
  }
}
