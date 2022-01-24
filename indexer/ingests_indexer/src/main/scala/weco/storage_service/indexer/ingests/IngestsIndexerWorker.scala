package weco.storage_service.indexer.ingests

import akka.actor.ActorSystem
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import weco.monitoring.Metrics
import weco.storage_service.ingests.models.Ingest
import weco.storage_service.indexer.{Indexer, IndexerWorker, IndexerWorkerError}
import weco.storage_service.indexer.ingests.models.IndexedIngest

import scala.concurrent.Future

class IngestsIndexerWorker(
  config: AlpakkaSQSWorkerConfig,
  val indexer: Indexer[Ingest, IndexedIngest]
)(
  implicit
  val actorSystem: ActorSystem,
  val sqsAsync: SqsAsyncClient,
  val metrics: Metrics[Future],
  val decoder: Decoder[Ingest]
) extends IndexerWorker[Ingest, Ingest, IndexedIngest](config, indexer) {

  def load(source: Ingest): Future[Either[IndexerWorkerError, Ingest]] =
    Future.successful(Right(source))
}
