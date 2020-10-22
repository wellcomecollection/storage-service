package uk.ac.wellcome.platform.archive.indexer.files

import akka.actor.ActorSystem
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.messaging.worker.models.{
  NonDeterministicFailure,
  Result,
  Successful
}
import uk.ac.wellcome.messaging.worker.monitoring.metrics.MetricsMonitoringClient
import uk.ac.wellcome.platform.archive.indexer.elasticsearch._
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.models.FileContext
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
) extends IndexerWorker[Seq[FileContext], FileContext, IndexedFile](
      config,
      indexer,
      metricsNamespace
    ) {

  override def process(contexts: Seq[FileContext]): Future[Result[Unit]] =
    indexer.index(contexts).map {
      case Right(successfulDocuments) =>
        debug(s"Successfully indexed $successfulDocuments")
        Successful(None)
      case Left(failedDocuments) =>
        warn(s"RetryableIndexingError: Unable to index $failedDocuments")
        NonDeterministicFailure(
          new Throwable(s"Unable to index ${failedDocuments.size} documents")
        )
    }

  override def load(
    source: Seq[FileContext]
  ): Future[Either[IndexerWorkerError, FileContext]] =
    Future.failed(new Throwable("Should not be called"))
}
