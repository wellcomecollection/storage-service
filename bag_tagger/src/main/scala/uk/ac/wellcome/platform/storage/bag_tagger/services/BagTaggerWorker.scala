package uk.ac.wellcome.platform.storage.bag_tagger.services

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
import uk.ac.wellcome.messaging.worker.models.{Result, Successful}
import uk.ac.wellcome.messaging.worker.monitoring.metrics.{
  MetricsMonitoringClient,
  MetricsMonitoringProcessor
}
import uk.ac.wellcome.platform.archive.common.BagRegistrationNotification

import scala.concurrent.{ExecutionContext, Future}

import uk.ac.wellcome.typesafe.Runnable

case class BagTaggerOutput(foo: String)

class BagTaggerWorker(
  val config: AlpakkaSQSWorkerConfig,
  val metricsNamespace: String
)(
  implicit
  val mc: MetricsMonitoringClient,
  val as: ActorSystem,
  val sc: SqsAsyncClient,
  val wd: Decoder[BagRegistrationNotification]
) extends Runnable
    with Logging {

  implicit val ec = as.dispatcher

  def process(sourceT: BagRegistrationNotification): Future[Result[Unit]] =
    Future {
      Successful(None)
    }

  val worker
    : AlpakkaSQSWorker[BagRegistrationNotification, Instant, Instant, Unit] =
    new AlpakkaSQSWorker[BagRegistrationNotification, Instant, Instant, Unit](
      config,
      monitoringProcessorBuilder = (ec: ExecutionContext) =>
        new MetricsMonitoringProcessor[BagRegistrationNotification](
          metricsNamespace
        )(mc, ec)
    )(process) {
      override val retryAction: Message => sqs.MessageAction =
        (message: Message) =>
          MessageAction.changeMessageVisibility(message, visibilityTimeout = 0)
    }

  override def run(): Future[Any] = worker.start
}
