package uk.ac.wellcome.platform.archive.indexer.elasticsearch

import java.time.Instant

import akka.actor.ActorSystem
import akka.stream.alpakka.sqs
import akka.stream.alpakka.sqs.MessageAction
import grizzled.slf4j.Logging
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.Message
import uk.ac.wellcome.messaging.sqsworker.alpakka.{
  AlpakkaSQSWorker,
  AlpakkaSQSWorkerConfig
}
import uk.ac.wellcome.messaging.worker.models.{
  NonDeterministicFailure,
  Result,
  Successful
}
import uk.ac.wellcome.messaging.worker.monitoring.metrics.{
  MetricsMonitoringClient,
  MetricsMonitoringProcessor
}
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

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

  private def index(t: T): Future[Result[Unit]] =
    indexer
      .index(Seq(t))
      .map {
        case Right(_) =>
          debug(s"Successfully indexed $t")
          Successful(None)

        // We can't be sure what the error is here.  The cost of retrying it is
        // very cheap, so assume it's a flaky error and let it land on the DLQ if not.
        case Left(t) =>
          warn(s"Unable to index $t")
          NonDeterministicFailure(new Throwable(s"Error indexing $t"))
      }

  def load(source: SourceT): Future[T]

  def process(sourceT: SourceT): Future[Result[Unit]] =
    for {
      t <- load(sourceT)
      result <- index(t)
    } yield result

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
