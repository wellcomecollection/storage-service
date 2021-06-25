package weco.storage_service.indexer

import akka.actor.ActorSystem
import akka.stream.alpakka.sqs
import akka.stream.alpakka.sqs.MessageAction
import cats.data.EitherT
import cats.implicits._
import grizzled.slf4j.Logging
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.Message
import weco.messaging.sqsworker.alpakka.{AlpakkaSQSWorker, AlpakkaSQSWorkerConfig}
import weco.messaging.worker.models.{DeterministicFailure, NonDeterministicFailure, Result, Successful}
import weco.messaging.worker.monitoring.metrics.MetricsMonitoringProcessor
import weco.monitoring.Metrics
import weco.typesafe.Runnable

import java.time.Instant
import scala.concurrent.{ExecutionContext, Future}

abstract class IndexerWorker[SourceT, T, IndexedT](
  config: AlpakkaSQSWorkerConfig,
  indexer: Indexer[T, IndexedT],
  metricsNamespace: String
)(
  implicit
  actorSystem: ActorSystem,
  sqsAsync: SqsAsyncClient,
  metrics: Metrics[Future],
  decoder: Decoder[SourceT]
) extends Runnable
    with Logging {

  implicit val ec: ExecutionContext = actorSystem.dispatcher

  private def indexT(t: T): Future[Either[IndexerWorkerError, T]] =
    indexer.index(Seq(t)) map {
      case Right(_) => Right(t)
      // We can't be sure what the error is here.  The cost of retrying it is
      // very cheap, so assume it's a flaky error and can be retried.
      case Left(_) =>
        Left(
          RetryableIndexingError(
            payload = t,
            cause = new Exception(s"Error indexing ${indexer.id(t)}")
          )
        )
    }

  def load(source: SourceT): Future[Either[IndexerWorkerError, T]]

  def process(sourceT: SourceT): Future[Result[Unit]] =
    (for {
      t <- EitherT(load(sourceT))
      result <- EitherT(indexT(t))
    } yield result).value map {
      case Right(t) =>
        debug(s"Successfully indexed $t")
        Successful(None)
      case Left(RetryableIndexingError(t, e)) =>
        warn(s"RetryableIndexingError: Unable to index $t")
        NonDeterministicFailure(e)
      case Left(e @ FatalIndexingError(t)) =>
        warn(s"FatalIndexingError: Unable to index $t")
        DeterministicFailure(e)
    }

  val worker: AlpakkaSQSWorker[SourceT, Instant, Instant, Unit] =
    new AlpakkaSQSWorker[SourceT, Instant, Instant, Unit](
      config,
      monitoringProcessorBuilder = (ec: ExecutionContext) =>
        new MetricsMonitoringProcessor[SourceT](metricsNamespace)(metrics, ec)
    )(process) {
      // If we retry set a non-zero visibility timeout to give
      // whatever dependency isn't working time to recover
      val visibilityTimeoutInSeconds = 5

      override val retryAction: Message => sqs.MessageAction =
        (message: Message) =>
          MessageAction.changeMessageVisibility(
            message = message,
            visibilityTimeout = visibilityTimeoutInSeconds
          )
    }

  def run(): Future[Any] = worker.start
}
