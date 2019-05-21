package uk.ac.wellcome.platform.archive.ingests.services

import akka.actor.ActorSystem
import com.amazonaws.services.sqs.AmazonSQSAsync
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sqsworker.alpakka.{AlpakkaSQSWorker, AlpakkaSQSWorkerConfig}
import uk.ac.wellcome.messaging.worker.models.{DeterministicFailure, Result, Successful}
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestUpdate}
import uk.ac.wellcome.platform.archive.common.ingests.monitor.DynamoIngestTracker
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class IngestsWorker[MessageDestination](
  alpakkaSQSWorkerConfig: AlpakkaSQSWorkerConfig,
  ingestTracker: DynamoIngestTracker,
  callbackNotificationService: CallbackNotificationService[MessageDestination]
)(implicit
  actorSystem: ActorSystem,
  mc: MonitoringClient,
  sc: AmazonSQSAsync)
    extends Runnable
    with Logging {

  private val worker =
    AlpakkaSQSWorker[IngestUpdate, Ingest](alpakkaSQSWorkerConfig) {
      update => Future.fromTry { processMessage(update) }
    }

  def processMessage(ingestUpdate: IngestUpdate): Try[Result[Ingest]] = Try {
    val result = for {
      ingest <- ingestTracker.update(ingestUpdate)
      _ <- callbackNotificationService
        .sendNotification(ingest)
    } yield ingest

    result match {
      case Success(ingest) => Successful(Some(ingest))
      case Failure(err) => DeterministicFailure(err, summary = None)
    }
  }

  override def run(): Future[Any] = worker.start
}
