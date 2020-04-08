package uk.ac.wellcome.platform.archive.indexer.ingests

import akka.actor.ActorSystem
import akka.stream.alpakka.sqs
import akka.stream.alpakka.sqs.MessageAction
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model.Message
import grizzled.slf4j.Logging
import io.circe.Decoder
import uk.ac.wellcome.messaging.sqsworker.alpakka.{AlpakkaSQSWorker, AlpakkaSQSWorkerConfig}
import uk.ac.wellcome.messaging.worker.models.{NonDeterministicFailure, Result, Successful}
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class IngestsIndexerWorker(
  val config: AlpakkaSQSWorkerConfig,
  ingestIndexer: IngestIndexer
)(
  implicit
  val monitoringClient: MonitoringClient,
  val actorSystem: ActorSystem,
  val sqsAsync: AmazonSQSAsync,
  decoder: Decoder[Ingest]
) extends Runnable with Logging {

  implicit val ec: ExecutionContext = actorSystem.dispatcher

  def process(ingest: Ingest): Future[Result[Unit]] =
    ingestIndexer
      .index(Seq(ingest)).map {
        case Right(_) =>
          debug(
            s"Successfully indexed ${ingest.id} " +
            s"(modified ${ingest.lastModifiedDate.getOrElse(ingest.createdDate)})")
          Successful(None)

        // We can't be sure what the error is here.  The cost of retrying it is
        // very cheap, so assume it's a flaky error and let it land on the DLQ if not.
        case Left(ingests) =>
          warn(s"Unable to index ${ingest.id}")
          NonDeterministicFailure(
            new Throwable(s"Error indexing ${ingest.id}"), summary = None
          )
      }

  val worker: AlpakkaSQSWorker[Ingest, Unit, MonitoringClient] =
    new AlpakkaSQSWorker[Ingest, Unit, MonitoringClient](config)(process) {
      override val retryAction: Message => (Message, sqs.MessageAction) =
        (_, MessageAction.changeMessageVisibility(0))
    }

  def run(): Future[Any] = worker.start
}