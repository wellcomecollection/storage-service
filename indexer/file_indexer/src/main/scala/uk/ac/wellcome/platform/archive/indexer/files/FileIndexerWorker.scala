package uk.ac.wellcome.platform.archive.indexer.files

import akka.actor.ActorSystem
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.messaging.worker.monitoring.metrics.MetricsMonitoringClient
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.models.FileContext
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.{Indexer, IndexerWorker, IndexerWorkerError}
import uk.ac.wellcome.platform.archive.indexer.files.models.IndexedFile

import scala.concurrent.Future

class FileIndexerWorker(
  val config: AlpakkaSQSWorkerConfig,
  val indexer: Indexer[FileContext, IndexedFile],
  val metricsNamespace: String
)(
  implicit
  val actorSystem: ActorSystem,
  val sqsAsync: SqsAsyncClient,
  val monitoringClient: MetricsMonitoringClient,
  val decoder: Decoder[FileContext]
) extends IndexerWorker[FileContext, FileContext, IndexedFile](
  config,
  indexer,
  metricsNamespace
) {

  def load(source: FileContext): Future[Either[IndexerWorkerError, FileContext]] =
    Future.successful(Right(source))
}
