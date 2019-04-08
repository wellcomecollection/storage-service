package uk.ac.wellcome.platform.archive.ingests.services

import akka.actor.ActorSystem
import com.amazonaws.services.sqs.AmazonSQSAsync
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sqsworker.alpakka.{AlpakkaSQSWorker, AlpakkaSQSWorkerConfig}
import uk.ac.wellcome.messaging.worker.models.{DeterministicFailure, Result, Successful}
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestUpdate}
import uk.ac.wellcome.platform.archive.common.ingests.monitor.IngestTracker
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class NewIngestsWorker(
  alpakkaSQSWorkerConfig: AlpakkaSQSWorkerConfig,
  ingestTracker: IngestTracker,
  callbackNotificationService: CallbackNotificationService
)(
  implicit
  actorSystem: ActorSystem,
  ec: ExecutionContext,
  mc: MonitoringClient,
  sc: AmazonSQSAsync)
  extends Runnable
  with Logging {

  private val worker: AlpakkaSQSWorker[IngestUpdate, Ingest] =
    AlpakkaSQSWorker[IngestUpdate, Ingest](alpakkaSQSWorkerConfig) {
      processMessage
    }

  def processMessage(ingestUpdate: IngestUpdate): Future[Result[Ingest]] = {
    val future = for {
      ingest <- Future.fromTry(
        ingestTracker.update(ingestUpdate)
      )
      _ <- callbackNotificationService
        .sendNotification(ingest)
    } yield ingest

    future
      .map { ingest => Successful(Some(ingest)) }
      .recover { case throwable => DeterministicFailure(throwable, summary = None) }
  }

  override def run(): Future[Any] = worker.start
}
