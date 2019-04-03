package uk.ac.wellcome.platform.archive.common.operation.services

import io.circe.Encoder
import uk.ac.wellcome.messaging.sns.SNSWriter
import uk.ac.wellcome.platform.archive.common.storage.models.{IngestCompleted, IngestFailed, IngestStepResult, IngestStepSuccess}

import scala.concurrent.{ExecutionContext, Future}

class OutgoingPublisher(
  operationName: String,
  snsWriter: SNSWriter
) {
  def sendIfSuccessful[R, O](result: IngestStepResult[R], outgoing: => O)(
    implicit
    ec: ExecutionContext,
    enc: Encoder[O]): Future[Unit] = {
    result match {
      case IngestStepSuccess(_) | IngestCompleted(_) =>
        send(outgoing)
      case IngestFailed(_, _, _) =>
        Future.successful(())
    }
  }

  private def send[O, R](outgoing: => O)(implicit
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
