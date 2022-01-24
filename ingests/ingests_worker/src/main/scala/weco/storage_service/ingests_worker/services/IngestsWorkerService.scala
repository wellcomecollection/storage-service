package weco.storage_service.ingests_worker.services

import akka.actor.ActorSystem
import grizzled.slf4j.Logging
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.json.JsonUtil._
import weco.messaging.sqsworker.alpakka.{
  AlpakkaSQSWorker,
  AlpakkaSQSWorkerConfig
}
import weco.messaging.worker.models.{
  DeterministicFailure,
  NonDeterministicFailure,
  Result,
  Successful
}
import weco.messaging.worker.monitoring.metrics.MetricsProcessor
import weco.monitoring.Metrics
import weco.storage_service.ingests.models.{Ingest, IngestUpdate}
import weco.storage_service.ingests_tracker.client.{
  IngestTrackerClient,
  IngestTrackerUnknownUpdateError,
  IngestTrackerUpdateConflictError,
  IngestTrackerUpdateNonExistentIngestError
}
import weco.typesafe.Runnable

import java.time.Instant
import scala.concurrent.{ExecutionContextExecutor, Future}

class IngestsWorkerService(
  config: AlpakkaSQSWorkerConfig,
  ingestTrackerClient: IngestTrackerClient
)(
  implicit actorSystem: ActorSystem,
  mc: Metrics[Future],
  sc: SqsAsyncClient
) extends Runnable
    with Logging {

  implicit val ec: ExecutionContextExecutor = actorSystem.dispatcher

  private val worker =
    new AlpakkaSQSWorker[IngestUpdate, Instant, Instant, Ingest](
      config, new MetricsProcessor(config.metricsConfig.namespace))(processMessage)

  def processMessage(ingestUpdate: IngestUpdate): Future[Result[Ingest]] = {
    ingestTrackerClient.updateIngest(ingestUpdate).map {
      case Right(ingest) =>
        info(f"Successfully applied $ingestUpdate, got $ingest")
        Successful(Some(ingest))
      case Left(IngestTrackerUpdateConflictError(_)) =>
        val err = new Exception(
          f"Error trying to apply update $ingestUpdate, got Conflict"
        )
        warn(err)
        DeterministicFailure[Ingest](err)
      // This may be caused by something like a consistency issue in DynamoDB;
      // if we retry later the ingest may become available.
      // See https://github.com/wellcomecollection/platform/issues/4781
      case Left(IngestTrackerUpdateNonExistentIngestError(_)) =>
        error(s"Could not apply $ingestUpdate to non-existent ingest")
        NonDeterministicFailure[Ingest](
          new Throwable(s"Could not apply $ingestUpdate to non-existent ingest")
        )
      case Left(IngestTrackerUnknownUpdateError(_, err)) =>
        error(s"Error trying to apply $ingestUpdate, got UnknownError", err)
        NonDeterministicFailure[Ingest](err)
    } recover {
      case err =>
        warn(s"Error trying to apply update $ingestUpdate: $err")
        NonDeterministicFailure[Ingest](err)
    }
  }

  override def run(): Future[Any] = worker.start
}
