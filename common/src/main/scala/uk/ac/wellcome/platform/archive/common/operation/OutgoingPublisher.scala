package uk.ac.wellcome.platform.archive.common.operation

import io.circe.Encoder
import uk.ac.wellcome.messaging.sns.SNSWriter
import uk.ac.wellcome.platform.archive.common.ingests.operation.OperationResult

import scala.concurrent.{ExecutionContext, Future}

class OutgoingPublisher(
  operationName: String,
  snsWriter: SNSWriter
) {
  def sendIfSuccessful[R, O](result: OperationResult[R], outgoing: => O)(
    implicit
    ec: ExecutionContext,
    enc: Encoder[O]): Future[Unit] = {
    if (result.isSuccessful) {
      snsWriter
        .writeMessage(
          outgoing,
          subject = s"Sent by ${this.getClass.getSimpleName}"
        )
        .map(_ => ())
    } else {
      Future.successful(())
    }
  }

}
