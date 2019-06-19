package uk.ac.wellcome.platform.archive.common.operation.services

import io.circe.Json
import uk.ac.wellcome.messaging.MessageSender
import uk.ac.wellcome.platform.archive.common.storage.models._

import scala.util.{Success, Try}

class OutgoingPublisher[Destination](
  messageSender: MessageSender[Destination]
) {
  def sendIfSuccessful[R](result: IngestStepResult[R],
                          outgoing: Json): Try[Unit] = {
    result match {
      case IngestStepSucceeded(_) | IngestCompleted(_) =>
        messageSender.sendT(outgoing)
      case IngestFailed(_, _, _) =>
        Success(())
    }
  }
}
