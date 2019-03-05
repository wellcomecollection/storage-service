package uk.ac.wellcome.platform.archive.bagunpacker.services

import java.util.UUID

import grizzled.slf4j.Logging
import io.circe.Encoder
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.SNSWriter
import uk.ac.wellcome.platform.archive.bagunpacker.config.models.{OperationFailure, OperationResult, OperationSuccess}
import uk.ac.wellcome.platform.archive.common.progress.models.{Progress, ProgressEvent, ProgressStatusUpdate, ProgressUpdate}

import scala.concurrent.{ExecutionContext, Future}

class OperationNotifierService(
                                operationName: String,
                                outgoingSnsWriter: SNSWriter,
                                progressSnsWriter: SNSWriter
                              ) extends Logging {

  def send[R, T <: OperationResult[R], O](
                                           requestId: UUID,
                                           result: T)(
                                           outgoing: R => O)(implicit ec: ExecutionContext, encoder: Encoder[O]) = {

    val outgoingPublication: Future[Unit] = result match {
      case OperationSuccess(summary) =>
        sendOutgoing(outgoing(summary)).map(_ => ())
      case OperationFailure(_, _) =>
        Future.successful(())
    }

    val progressPublication: Future[Unit] =
      sendProgress[R,T](requestId, result)
        .map(_ => ())

    for {
      _ <- progressPublication
      _ <- outgoingPublication
    } yield ()
  }

  def sendOutgoing[O](outgoing: O)(implicit encoder: Encoder[O]) =
    outgoingSnsWriter.writeMessage(
      outgoing,
      subject =
        s"Sent by ${this.getClass.getSimpleName}"
    )

  def sendProgress[R, T <: OperationResult[R]](requestId: UUID, result: T) = {
    val update = result match {
      case OperationSuccess(summary) => {
        info(
          s"Success: $requestId: ${summary.toString}"
        )

        ProgressUpdate.event(
          id = requestId,
          description = s"${operationName.capitalize} succeeded"
        )
      }
      case OperationFailure(summary, e) => {
        error(
          s"Failure: $requestId: ${summary.toString}",
          e
        )

        ProgressStatusUpdate(
          id = requestId,
          status = Progress.Failed,
          affectedBag = None,
          events = List(
            ProgressEvent(s"${operationName.capitalize} failed")
          )
        )
      }
    }

    progressSnsWriter.writeMessage[ProgressUpdate](
      update,
      subject = s"Sent by ${this.getClass.getSimpleName}"
    )
  }
}
