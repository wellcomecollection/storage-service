package uk.ac.wellcome.platform.archive.common.storage.models

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
  DeterministicFailure,
  NonDeterministicFailure,
  Result,
  Successful
}
import uk.ac.wellcome.messaging.worker.monitoring.metrics.{
  MetricsMonitoringClient,
  MetricsMonitoringProcessor
}
import uk.ac.wellcome.platform.archive.common.PipelinePayload
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

sealed trait IngestStep[+T]

case class IngestStepStarted(
  id: IngestID
) extends IngestStep[IngestID]

sealed trait IngestStepResult[+T] extends IngestStep[T] {
  val summary: T
  val maybeUserFacingMessage: Option[String]
}

case class IngestCompleted[T](
  summary: T
) extends IngestStepResult[T] {
  override val maybeUserFacingMessage: Option[String] = None
}

case class IngestStepSucceeded[T](
  summary: T,
  maybeUserFacingMessage: Option[String] = None
) extends IngestStepResult[T]

case class IngestFailed[T](
  summary: T,
  e: Throwable,
  maybeUserFacingMessage: Option[String] = None
) extends IngestStepResult[T]

case class IngestShouldRetry[T](
  summary: T,
  e: Throwable,
  maybeUserFacingMessage: Option[String] = None
) extends IngestStepResult[T]

trait IngestStepWorker[Work <: PipelinePayload, Summary]
    extends Runnable
    with Logging {

  // TODO: Move visibilityTimeout into SQSConfig
  val config: AlpakkaSQSWorkerConfig
  val visibilityTimeout = 0

  implicit val mc: MetricsMonitoringClient
  implicit val as: ActorSystem
  implicit val wd: Decoder[Work]
  implicit val sc: SqsAsyncClient

  implicit val metricsNamespace: String

  def processMessage(payload: Work): Try[IngestStepResult[Summary]]

  def process(payload: Work): Future[Result[Summary]] = Future.fromTry {
    processMessage(payload).map(toResult)
  }

  val worker =
    new AlpakkaSQSWorker[Work, Instant, Instant, Summary](
      config,
      monitoringProcessorBuilder = (ec: ExecutionContext) =>
        new MetricsMonitoringProcessor[Work](metricsNamespace)(mc, ec)
    )(process) {
      override val retryAction: Message => sqs.MessageAction =
        (message: Message) =>
          MessageAction.changeMessageVisibility(message, visibilityTimeout)
    }

  def run(): Future[Any] = worker.start

  def toResult[T](ingestResult: IngestStepResult[T]): Result[T] =
    ingestResult match {
      case IngestStepSucceeded(s, _)  => Successful(Some(s))
      case IngestCompleted(s)         => Successful(Some(s))
      case IngestFailed(s, t, _)      => DeterministicFailure(t, Some(s))
      case IngestShouldRetry(s, t, _) => NonDeterministicFailure(t, Some(s))
    }
}
