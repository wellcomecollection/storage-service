package uk.ac.wellcome.platform.archive.common.operation.services

import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.MessageSender
import uk.ac.wellcome.platform.archive.common.PipelinePayload
import uk.ac.wellcome.platform.archive.common.storage.models._

import scala.util.{Success, Try}

class OutgoingPublisher[Destination](
  messageSender: MessageSender[Destination]
) extends Logging {
  def sendIfSuccessful[R, O <: PipelinePayload](
    result: IngestStepResult[R],
    outgoing: => O
  ): Try[Unit] = {
    debug(s"Sending outgoing message for result $result")
    result match {
      case IngestStepSucceeded(_, _) | IngestCompleted(_) =>
        debug(
          msg =
            "Ingest step succeeded/completed: " +
              s"sending an outgoing message $outgoing"
        )
        messageSender.sendT[PipelinePayload](outgoing)
      case IngestFailed(_, _, _) =>
        debug(s"Ingest step failed: not sending a message")
        Success(())
      case IngestShouldRetry(_, _, _) =>
        debug(s"Ingest step retrying: not sending a message")
        Success(())
    }
  }
}
