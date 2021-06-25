package weco.storage_service.operation.services

import grizzled.slf4j.Logging
import weco.json.JsonUtil._
import weco.messaging.MessageSender
import weco.storage_service.PipelinePayload
import weco.storage_service.storage.models._

import scala.util.{Success, Try}

class OutgoingPublisher[Destination](
  messageSender: MessageSender[Destination]
) extends Logging {

  def send(outgoing: PipelinePayload): Try[Unit] =
    messageSender.sendT[PipelinePayload](outgoing)

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

        send(outgoing)

      case IngestFailed(_, _, _) =>
        debug(s"Ingest step failed: not sending a message")
        Success(())

      case IngestShouldRetry(_, _, _) =>
        debug(s"Ingest step retrying: not sending a message")
        Success(())
    }
  }
}
