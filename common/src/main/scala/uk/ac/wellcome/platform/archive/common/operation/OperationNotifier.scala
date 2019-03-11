package uk.ac.wellcome.platform.archive.common.operation

import java.util.UUID

import grizzled.slf4j.Logging
import io.circe.Encoder
import uk.ac.wellcome.platform.archive.common.models.bagit.BagId

import scala.concurrent.{ExecutionContext, Future}

class OperationNotifier(
                         outgoing: OutgoingNotifier,
                         ingests: IngestNotifier
) extends Logging {

  def send[R, O](
    requestId: UUID,
    result: OperationResult[R],
    bagId: Option[BagId] = None
  )(
    outgoingTransform: R => O
  )(implicit
    ec: ExecutionContext,
    enc: Encoder[O]): Future[Unit] =
    for {
      _ <- ingests.send(requestId, result, bagId)
      _ <- outgoing.send(requestId, result)(outgoingTransform)
    } yield ()
}
