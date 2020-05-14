package uk.ac.wellcome.platform.storage.ingests_worker.services

import java.time.Instant

import akka.actor.ActorSystem
import grizzled.slf4j.Logging
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sqsworker.alpakka.{
  AlpakkaSQSWorker,
  AlpakkaSQSWorkerConfig
}
import uk.ac.wellcome.messaging.worker.models.{
  DeterministicFailure,
  NonDeterministicFailure,
  Result,
  Successful
}
import uk.ac.wellcome.messaging.worker.monitoring.metrics.{
  MetricsMonitoringClient,
  MetricsMonitoringProcessor
}
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Ingest,
  IngestUpdate
}
import uk.ac.wellcome.platform.storage.ingests_tracker.client.{
  IngestTrackerClient,
  IngestTrackerConflictError,
  IngestTrackerUnknownError
}
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

class IngestsWorkerService(
  workerConfig: AlpakkaSQSWorkerConfig,
  metricsNamespace: String,
  ingestTrackerClient: IngestTrackerClient
)(
  implicit actorSystem: ActorSystem,
  mc: MetricsMonitoringClient,
  sc: SqsAsyncClient
) extends Runnable
    with Logging {

  implicit val ec: ExecutionContextExecutor = actorSystem.dispatcher

  private val monitoringProcessorBuilder = (ec: ExecutionContext) =>
    new MetricsMonitoringProcessor[IngestUpdate](metricsNamespace)(mc, ec)

  private val worker =
    AlpakkaSQSWorker[IngestUpdate, Instant, Instant, Ingest](
      config = workerConfig,
      monitoringProcessorBuilder = monitoringProcessorBuilder
    )(processMessage)

  def processMessage(ingestUpdate: IngestUpdate): Future[Result[Ingest]] = {
    ingestTrackerClient.updateIngest(ingestUpdate).map {
      case Right(ingest) =>
        info(f"Successfully applied $ingestUpdate, got $ingest")
        Successful(Some(ingest))
      case Left(IngestTrackerConflictError(_)) =>
        val err = new Exception(
          f"Error trying to apply update $ingestUpdate, got Conflict"
        )
        warn(err)
        DeterministicFailure[Ingest](err)
      case Left(IngestTrackerUnknownError(_, err)) =>
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
