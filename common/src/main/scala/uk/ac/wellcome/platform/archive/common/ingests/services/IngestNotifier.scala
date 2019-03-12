package uk.ac.wellcome.platform.archive.common.ingests.services

import java.util.UUID

import grizzled.slf4j.Logging
import io.circe.Encoder
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.operation.services.{OperationResult, OutgoingPublisher}

import scala.concurrent.{ExecutionContext, Future}

class IngestNotifier(outgoing: OutgoingPublisher,
                     ingestUpdater: IngestUpdater)
    extends Logging {
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
      _ <- ingestUpdater.send(requestId, result, bagId)
      _ <- outgoing.send(requestId, result)(outgoingTransform)
    } yield ()
}
