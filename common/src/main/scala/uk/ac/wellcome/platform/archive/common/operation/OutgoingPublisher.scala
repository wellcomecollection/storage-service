package uk.ac.wellcome.platform.archive.common.operation

import io.circe.Encoder
import uk.ac.wellcome.messaging.sns.SNSWriter
import uk.ac.wellcome.platform.archive.common.ingests.operation.{OperationCompleted, OperationFailure, OperationResult, OperationSuccess}

import scala.concurrent.{ExecutionContext, Future}

class OutgoingPublisher(
  operationName: String,
  snsWriter: SNSWriter
) {
  def sendIfSuccessful[R, O](result: OperationResult[R], outgoing: => O)(
    implicit
    ec: ExecutionContext,
    enc: Encoder[O]): Future[Unit] = {
    result match {
      case OperationSuccess(_) | OperationCompleted(_) =>
        send(outgoing)
      case OperationFailure(_, _) =>
        Future.successful(())
    }
  }

  private def send[O, R](outgoing: => O)(
    implicit
    ec: ExecutionContext,
    enc: Encoder[O]): Future[Unit] = {
    snsWriter
      .writeMessage(
        outgoing,
        subject = s"Sent by ${this.getClass.getSimpleName}"
      )
      .map(_ => ())
  }
}
