package uk.ac.wellcome.platform.archive.indexer.ingests

import akka.actor.ActorSystem
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.messaging.worker.monitoring.metrics.MetricsMonitoringClient
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.{
  Indexer,
  IndexerWorker,
  IndexerWorkerError
}
import uk.ac.wellcome.platform.archive.indexer.ingests.models.IndexedIngest

import scala.concurrent.Future

class IngestsIndexerWorker(
  val config: AlpakkaSQSWorkerConfig,
  val indexer: Indexer[Ingest, IndexedIngest],
  val metricsNamespace: String
)(
  implicit
  val actorSystem: ActorSystem,
  val sqsAsync: SqsAsyncClient,
  val monitoringClient: MetricsMonitoringClient,
  val decoder: Decoder[Ingest]
) extends IndexerWorker[Ingest, Ingest, IndexedIngest](
      config,
      indexer,
      metricsNamespace
    ) {

  def load(source: Ingest): Future[Either[IndexerWorkerError, Ingest]] =
    Future.successful(Right(source))

}
