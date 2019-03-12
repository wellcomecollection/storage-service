package uk.ac.wellcome.platform.archive.bagunpacker.services

import akka.Done
import grizzled.slf4j.Logging
import io.circe.Encoder
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sqs.NotificationStream
import uk.ac.wellcome.platform.archive.bagunpacker.config.builders.BagLocationBuilder
import uk.ac.wellcome.platform.archive.bagunpacker.config.models.UnpackerConfig
import uk.ac.wellcome.platform.archive.common.ingests.models.{BagRequest, UnpackBagRequest}
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestNotifier
import uk.ac.wellcome.platform.archive.common.operation.services.DiagnosticReporter
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class UnpackerWorker(
                      config: UnpackerConfig,
                      stream: NotificationStream[UnpackBagRequest],
                      notifier: IngestNotifier,
                      reporter: DiagnosticReporter,
                      unpacker: Unpacker,
)(implicit ec: ExecutionContext)
    extends Logging
    with Runnable {

  def run(): Future[Done] = stream.run(processMessage)

  def processMessage(request: UnpackBagRequest)(
    implicit enc: Encoder[BagRequest]): Future[Unit] = {
    val location = BagLocationBuilder.build(request, config)

    for {
      result <- unpacker.unpack(
        request.sourceLocation,
        location.objectLocation
      )

      _ <- reporter.report(request.requestId, result)

      _ <- notifier
        .send(request.requestId, result) { _ =>
          BagRequest(request.requestId, location)
        }

    } yield ()
  }
}
