package uk.ac.wellcome.platform.archive.common.operation.services

import java.util.UUID

import io.circe.Encoder
import uk.ac.wellcome.messaging.sns.{PublishAttempt, SNSWriter}

import scala.concurrent.{ExecutionContext, Future}

class OutgoingPublisher(
  operationName: String,
  snsWriter: SNSWriter
) {
  def send[R, O](
    requestId: UUID,
    result: OperationResult[R],
  )(
    transform: R => O
  )(implicit
    ec: ExecutionContext,
    enc: Encoder[O]): Future[Unit] = result match {
    case OperationSuccess(summary) =>
      sendOutgoing(transform(summary)).map(_ => ())
    case OperationFailure(_, _) =>
      Future.successful(())
    case OperationCompleted(summary) =>
      sendOutgoing(transform(summary)).map(_ => ())
  }

  private def sendOutgoing[O](outgoing: O)(
    implicit encoder: Encoder[O]): Future[PublishAttempt] =
    snsWriter.writeMessage(
      outgoing,
      subject = s"Sent by ${this.getClass.getSimpleName}"
    )
}
