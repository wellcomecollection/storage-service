package uk.ac.wellcome.platform.archive.bagreplicator.services

import akka.Done
import uk.ac.wellcome.messaging.sqs.NotificationStream
import uk.ac.wellcome.platform.archive.common.ingests.models.BagRequest
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services.{DiagnosticReporter, OutgoingPublisher}
import uk.ac.wellcome.typesafe.Runnable

import uk.ac.wellcome.json.JsonUtil._

import scala.concurrent.{ExecutionContext, Future}

class BagReplicatorWorker(
  stream: NotificationStream[BagRequest],
  ingestUpdater: IngestUpdater,
  outgoing: OutgoingPublisher,
  reporter: DiagnosticReporter,
  replicator: BagReplicator)(implicit ec: ExecutionContext)
    extends Runnable {

  def run(): Future[Done] = stream.run(processMessage)

  def processMessage(request: BagRequest): Future[Unit] =
    for {
      result <- replicator.replicate(
        request.bagLocation
      )
      _ <- reporter.report(request.requestId, result)
      _ <- ingestUpdater.send(request.requestId, result)
      _ <- outgoing.send(request.requestId, result) { summary =>
        BagRequest(
          requestId = request.requestId,
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
