package uk.ac.wellcome.platform.archive.bagreplicator.services

import akka.Done
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sqs.NotificationStream
import uk.ac.wellcome.platform.archive.common.models.BagRequest
import uk.ac.wellcome.platform.archive.common.operation.OperationNotifier
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class BagReplicatorWorker(
  stream: NotificationStream[BagRequest],
  notifier: OperationNotifier,
  replicator: BagReplicator
)(implicit ec: ExecutionContext)
    extends Runnable {

  def run(): Future[Done] = stream.run(processMessage)

  def processMessage(bagRequest: BagRequest): Future[Unit] =
    for {
      summary <- replicator.replicate(
        bagRequest.bagLocation
      )
      _ <- notifier.send(
        requestId = bagRequest.requestId,
        result = summary
      ) { summary =>
        BagRequest(
          requestId = bagRequest.requestId,
          bagLocation = summary.destination
            .getOrElse(
              throw new RuntimeException(
                "No destination provided by replication!"
              )
            )
        )
      }
    } yield ()
}
