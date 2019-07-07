package uk.ac.wellcome.platform.archive.ingests.services

import akka.actor.ActorSystem
import com.amazonaws.services.sqs.AmazonSQSAsync
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sqsworker.alpakka.{
  AlpakkaSQSWorker,
  AlpakkaSQSWorkerConfig
}
import uk.ac.wellcome.messaging.worker.models.{
  DeterministicFailure,
  Result,
  Successful
}
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Ingest,
  IngestUpdate
}
import uk.ac.wellcome.platform.archive.common.ingests.tracker.IngestTracker
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class IngestsWorker[CallbackDestination](
  alpakkaSQSWorkerConfig: AlpakkaSQSWorkerConfig,
  ingestTracker: IngestTracker,
  callbackNotificationService: CallbackNotificationService[CallbackDestination]
)(implicit actorSystem: ActorSystem, mc: MonitoringClient, sc: AmazonSQSAsync)
    extends Runnable
    with Logging {

  private val worker =
    AlpakkaSQSWorker[IngestUpdate, Ingest](alpakkaSQSWorkerConfig) { payload =>
      Future.fromTry { processMessage(payload) }
    }

  def processMessage(ingestUpdate: IngestUpdate): Try[Result[Ingest]] = {
    val result = for {
      ingest <- ingestTracker.update(ingestUpdate) match {
        case Right(updatedIngest) => Success(updatedIngest.identifiedT)
        case Left(err) =>
          Failure(new Throwable(s"Error from the ingest tracker: $err"))
      }
      _ <- callbackNotificationService.sendNotification(ingest)
    } yield ingest

    result match {
      case Success(ingest) => Success(Successful(Some(ingest)))
      case Failure(err) => Success(DeterministicFailure(err, summary = None))
    }
  }

  override def run(): Future[Any] = worker.start
}
