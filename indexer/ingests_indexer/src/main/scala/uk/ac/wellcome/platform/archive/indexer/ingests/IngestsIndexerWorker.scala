package uk.ac.wellcome.platform.archive.indexer.ingests

import java.time.Instant

import akka.actor.ActorSystem
import akka.stream.alpakka.sqs
import akka.stream.alpakka.sqs.MessageAction
import grizzled.slf4j.Logging
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.Message
import uk.ac.wellcome.messaging.sqsworker.alpakka.{AlpakkaSQSWorker, AlpakkaSQSWorkerConfig}
import uk.ac.wellcome.messaging.worker.models.{NonDeterministicFailure, Result, Successful}
import uk.ac.wellcome.messaging.worker.monitoring.metrics.{MetricsMonitoringClient, MetricsMonitoringProcessor}
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class IngestsIndexerWorker(
  val config: AlpakkaSQSWorkerConfig,
  ingestIndexer: IngestIndexer,
  metricsNamespace: String
)(
  implicit
  val monitoringClient: MetricsMonitoringClient,
  val actorSystem: ActorSystem,
  val sqsAsync: SqsAsyncClient,
  decoder: Decoder[Ingest]
) extends Runnable
    with Logging {

  implicit val ec: ExecutionContext = actorSystem.dispatcher

  def process(ingest: Ingest): Future[Result[Unit]] =
    ingestIndexer
      .index(Seq(ingest))
      .map {
        case Right(_) =>
          debug(
            s"Successfully indexed ${ingest.id} " +
              s"(modified ${ingest.lastModifiedDate.getOrElse(ingest.createdDate)})"
          )
          Successful(None)

        // We can't be sure what the error is here.  The cost of retrying it is
        // very cheap, so assume it's a flaky error and let it land on the DLQ if not.
        case Left(ingests) =>
          warn(s"Unable to index ${ingest.id}")
          NonDeterministicFailure(
            new Throwable(s"Error indexing ${ingest.id}"),
            summary = None
          )
      }

  val worker: AlpakkaSQSWorker[Ingest, Instant, Instant, Unit] =
    new AlpakkaSQSWorker[Ingest, Instant, Instant, Unit](
      config,
      monitoringProcessorBuilder = (ec: ExecutionContext) =>
        new MetricsMonitoringProcessor[Ingest](metricsNamespace)(monitoringClient, ec)
    )(process) {
      override val retryAction: Message => sqs.MessageAction =
        (message: Message) => MessageAction.changeMessageVisibility(message, visibilityTimeout = 0)
    }

  def run(): Future[Any] = worker.start
}
