package uk.ac.wellcome.platform.archive.common.ingests.operation

import java.util.UUID

import grizzled.slf4j.Logging
import io.circe.Encoder
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.SNSWriter
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Ingest,
  IngestEvent,
  IngestStatusUpdate,
  IngestUpdate
}
import uk.ac.wellcome.platform.archive.common.models.bagit.BagId

import scala.concurrent.{ExecutionContext, Future}

class OperationNotifier(
  operationName: String,
  outgoingSnsWriter: SNSWriter,
  ingestSnsWriter: SNSWriter
) extends Logging {

  def send[R, O](
    requestId: UUID,
    result: OperationResult[R],
    bagId: Option[BagId] = None
  )(
    outgoing: R => O
  )(implicit
    ec: ExecutionContext,
    enc: Encoder[O]) = {

    val outgoingPublication: Future[Unit] = result match {
      case OperationSuccess(summary) =>
        sendOutgoing(outgoing(summary)).map(_ => ())
      case OperationFailure(_, _) =>
        Future.successful(())
      case OperationCompleted(summary) =>
        sendOutgoing(outgoing(summary)).map(_ => ())
    }

    val ingestPublication: Future[Unit] =
      sendIngest[R](requestId, result, bagId)
        .map(_ => ())

    for {
      _ <- ingestPublication
      _ <- outgoingPublication
    } yield ()
  }

  private def sendOutgoing[O](outgoing: O)(implicit encoder: Encoder[O]) =
    outgoingSnsWriter.writeMessage(
      outgoing,
      subject = s"Sent by ${this.getClass.getSimpleName}"
    )

  private def sendIngest[R](
    requestId: UUID,
    result: OperationResult[R],
    bagId: Option[BagId]
  ) = {
    val update = result match {
      case OperationCompleted(summary) => {
        info(s"Completed - $requestId : ${summary.toString}")

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
      }

      case OperationSuccess(summary) => {
        info(s"Success - $requestId: ${summary.toString}")

        IngestUpdate.event(
          id = requestId,
          description = s"${operationName.capitalize} succeeded"
        )
      }

      case OperationFailure(summary, e) => {
        error(s"Failure - $requestId : ${summary.toString}", e)

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
    }

    ingestSnsWriter.writeMessage[IngestUpdate](
      update,
      subject = s"Sent by ${this.getClass.getSimpleName}")
  }
}
