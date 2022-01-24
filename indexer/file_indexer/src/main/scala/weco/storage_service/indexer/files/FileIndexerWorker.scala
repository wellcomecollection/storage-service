package weco.storage_service.indexer.files

import akka.actor.ActorSystem
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import weco.messaging.worker.models.{
  NonDeterministicFailure,
  Result,
  Successful
}
import weco.monitoring.Metrics
import weco.storage_service.indexer._
import weco.storage_service.indexer.models.FileContext
import weco.storage_service.indexer.files.models.IndexedFile

import scala.concurrent.Future

class FileIndexerWorker(
  config: AlpakkaSQSWorkerConfig,
  val indexer: Indexer[FileContext, IndexedFile]
)(
  implicit
  val actorSystem: ActorSystem,
  val sqsAsync: SqsAsyncClient,
  val metrics: Metrics[Future],
  val decoder: Decoder[FileContext]
) extends IndexerWorker[Seq[FileContext], FileContext, IndexedFile](
      config,
      indexer
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
