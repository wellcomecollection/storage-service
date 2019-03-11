package uk.ac.wellcome.platform.archive.bag_register.services

import akka.Done
import grizzled.slf4j.Logging
import io.circe.Encoder
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sqs.NotificationStream
import uk.ac.wellcome.platform.archive.common.models.BagRequest
import uk.ac.wellcome.platform.archive.common.operation.{OperationNotifier, OperationReporter}
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class BagRegisterWorker(
                         stream: NotificationStream[BagRequest],
                         notifier: OperationNotifier,
                         reporter: OperationReporter,
                         register: Register
)(implicit ec: ExecutionContext)
    extends Logging
    with Runnable {

  def run(): Future[Done] =
    stream.run(processMessage)

  def processMessage(
    request: BagRequest
  )(implicit
    enc: Encoder[BagRequest]): Future[Unit] = {
    for {
      result <- register.update(
        request.bagLocation
      )

      _ <- reporter.report(request.requestId, result)

      _ <- notifier.send(
        request.requestId,
        result,
        result.summary.bagId
      )(_ => request)
    } yield ()
  }
}
