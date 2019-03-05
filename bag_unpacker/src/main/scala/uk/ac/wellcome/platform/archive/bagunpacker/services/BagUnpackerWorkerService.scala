package uk.ac.wellcome.platform.archive.bagunpacker.services

import akka.Done
import grizzled.slf4j.Logging
import io.circe.Encoder
import uk.ac.wellcome.messaging.sqs.NotificationStream
import uk.ac.wellcome.platform.archive.bagunpacker.config.builders.BagLocationBuilder
import uk.ac.wellcome.platform.archive.bagunpacker.config.models.{
  BagUnpackerConfig,
  OperationResult
}
import uk.ac.wellcome.platform.archive.bagunpacker.models.UnpackSummary
import uk.ac.wellcome.platform.archive.common.models.{
  BagRequest,
  UnpackBagRequest
}
import uk.ac.wellcome.typesafe.Runnable
import uk.ac.wellcome.json.JsonUtil._

import scala.concurrent.{ExecutionContext, Future}

class BagUnpackerWorkerService(
  bagUnpackerConfig: BagUnpackerConfig,
  stream: NotificationStream[UnpackBagRequest],
  operationNotifier: OperationNotifierService,
  unpackerService: UnpackerService
)(implicit ec: ExecutionContext)
    extends Logging
    with Runnable {

  def run(): Future[Done] =
    stream.run(processMessage)

  def processMessage(
    unpackBagRequest: UnpackBagRequest
  )(implicit encoder: Encoder[BagRequest]): Future[Unit] = {

    val bagLocation = BagLocationBuilder.build(
      unpackBagRequest,
      bagUnpackerConfig
    )

    for {
      unpackResult <- unpackerService.unpack(
        unpackBagRequest.sourceLocation,
        bagLocation.objectLocation
      )

      _ <- operationNotifier
        .send[
          UnpackSummary,
          OperationResult[UnpackSummary],
          BagRequest
        ](
          unpackBagRequest.requestId,
          unpackResult
        ) { _ =>
          BagRequest(
            unpackBagRequest.requestId,
            bagLocation
          )
        }

    } yield ()
  }
}
