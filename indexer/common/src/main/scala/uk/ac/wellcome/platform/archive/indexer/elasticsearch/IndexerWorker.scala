package uk.ac.wellcome.platform.archive.indexer.elasticsearch

import java.time.Instant

import akka.actor.ActorSystem
import akka.stream.alpakka.sqs
import akka.stream.alpakka.sqs.MessageAction
import grizzled.slf4j.Logging
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.Message
import uk.ac.wellcome.messaging.sqsworker.alpakka.{AlpakkaSQSWorker, AlpakkaSQSWorkerConfig}
import uk.ac.wellcome.messaging.worker.models.{DeterministicFailure, NonDeterministicFailure, Result, Successful}
import uk.ac.wellcome.messaging.worker.monitoring.metrics.{MetricsMonitoringClient, MetricsMonitoringProcessor}
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}
import cats.data.EitherT
import cats.implicits._

abstract class IndexerWorker[SourceT, T, IndexedT](
  config: AlpakkaSQSWorkerConfig,
  indexer: Indexer[T, IndexedT],
  metricsNamespace: String
)(
  implicit
  actorSystem: ActorSystem,
  sqsAsync: SqsAsyncClient,
  monitoringClient: MetricsMonitoringClient,
  decoder: Decoder[SourceT]
) extends Runnable
    with Logging {

  implicit val ec: ExecutionContext = actorSystem.dispatcher

  private def indexT(t: T): Future[Either[IndexerWorkerError, T]] =
    indexer.index(Seq(t)) map {
      case Right(_) => Right(t)
      // We can't be sure what the error is here.  The cost of retrying it is
      // very cheap, so assume it's a flaky error and can be retried.
      case Left(_) => Left(
        RetryableIndexingError(
          payload = t,
          cause = new Exception(s"Error indexing $t")
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
      case Left(RetryableIndexingError(t,e)) =>
        warn(s"RetryableIndexingError: Unable to index $t")
        NonDeterministicFailure(e)
      case Left(e@FatalIndexingError(t)) =>
        warn(s"FatalIndexingError: Unable to index $t")
        DeterministicFailure(e)
    }

  val worker: AlpakkaSQSWorker[SourceT, Instant, Instant, Unit] =
    new AlpakkaSQSWorker[SourceT, Instant, Instant, Unit](
      config,
      monitoringProcessorBuilder = (ec: ExecutionContext) =>
        new MetricsMonitoringProcessor[SourceT](metricsNamespace)(
          monitoringClient,
          ec
        )
    )(process) {
      override val retryAction: Message => sqs.MessageAction =
        (message: Message) =>
          MessageAction.changeMessageVisibility(message, visibilityTimeout = 0)
    }

  def run(): Future[Any] = worker.start
}
