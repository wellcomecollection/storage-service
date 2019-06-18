package uk.ac.wellcome.platform.archive.common.operation.services

import grizzled.slf4j.Logging
import io.circe.Encoder
import uk.ac.wellcome.messaging.MessageSender
import uk.ac.wellcome.platform.archive.common.storage.models._

import scala.util.{Success, Try}

class OutgoingPublisher[Destination](
  messageSender: MessageSender[Destination]
) extends Logging {
  def sendIfSuccessful[R, O](result: IngestStepResult[R], outgoing: => O)(
    implicit enc: Encoder[O]): Try[Unit] = {
    debug(s"Sending outgoing message for result $result")
    result match {
      case IngestStepSucceeded(_) | IngestCompleted(_) =>
        debug(
          s"Ingest success/completed: sending an outgoing message $outgoing")
        messageSender.sendT(outgoing)
      case IngestFailed(_, _, _) =>
        debug(s"Ingest failed: not sending a message")
        Success(())
    }
  }
}
