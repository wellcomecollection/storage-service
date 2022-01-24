package weco.storage_service.storage.models

import akka.actor.ActorSystem
import akka.stream.alpakka.sqs
import akka.stream.alpakka.sqs.MessageAction
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.Message
import weco.messaging.sqsworker.alpakka.{
  AlpakkaSQSWorker,
  AlpakkaSQSWorkerConfig
}
import weco.messaging.worker.models.{
  Result,
  RetryableFailure,
  Successful,
  TerminalFailure
}
import weco.monitoring.Metrics
import weco.storage_service.PipelinePayload
import weco.storage_service.ingests.models.IngestID
import weco.typesafe.Runnable

import scala.concurrent.Future
import scala.concurrent.duration._
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
    extends Runnable {

  // TODO: Move visibilityTimeout into SQSConfig
  val config: AlpakkaSQSWorkerConfig
  val visibilityTimeout: Duration = 0.seconds

  implicit val mc: Metrics[Future]
  implicit val as: ActorSystem
  implicit val wd: Decoder[Work]
  implicit val sc: SqsAsyncClient

  implicit val metricsNamespace: String

  def processMessage(payload: Work): Try[IngestStepResult[Summary]]

  def process(payload: Work): Future[Result[Summary]] = Future.fromTry {
    processMessage(payload).map(toResult)
  }

  val worker: AlpakkaSQSWorker[Work, Summary] =
    new AlpakkaSQSWorker[Work, Summary](config)(process) {
      override val retryAction: Message => sqs.MessageAction =
        (message: Message) =>
          MessageAction.changeMessageVisibility(
            message,
            visibilityTimeout.toSeconds.toInt
          )
    }

  def run(): Future[Any] = worker.start

  def toResult[T](ingestResult: IngestStepResult[T]): Result[T] =
    ingestResult match {
      case IngestStepSucceeded(s, _)  => Successful(Some(s))
      case IngestCompleted(s)         => Successful(Some(s))
      case IngestFailed(s, t, _)      => TerminalFailure(t, Some(s))
      case IngestShouldRetry(s, t, _) => RetryableFailure(t, Some(s))
    }
}
