package uk.ac.wellcome.platform.archive.common.storage.models

import akka.actor.ActorSystem
import akka.stream.alpakka.sqs
import akka.stream.alpakka.sqs.MessageAction
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model.Message
import grizzled.slf4j.Logging
import io.circe.Decoder
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
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.common.PipelinePayload
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.Future
import scala.util.Try

sealed trait IngestStep[+T]

case class IngestStepStarted(
  id: IngestID
) extends IngestStep[IngestID]

sealed trait IngestStepResult[+T] extends IngestStep[T] {
  val summary: T
}

case class IngestCompleted[T](
  summary: T
) extends IngestStepResult[T]

case class IngestStepSucceeded[T](
  summary: T
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

  implicit val mc: MonitoringClient
  implicit val as: ActorSystem
  implicit val wd: Decoder[Work]
  implicit val sc: AmazonSQSAsync

  def processMessage(payload: Work): Try[IngestStepResult[Summary]]

  def process(payload: Work): Future[Result[Summary]] = Future.fromTry {
    processMessage(payload).map(toResult)
  }

  val worker =
    new AlpakkaSQSWorker[Work, Summary, MonitoringClient](config)(process) {
      override val retryAction: Message => (Message, sqs.MessageAction) =
        (_, MessageAction.changeMessageVisibility(visibilityTimeout))
    }

  def run(): Future[Any] = worker.start

  def toResult[T](ingestResult: IngestStepResult[T]): Result[T] =
    ingestResult match {
      case IngestStepSucceeded(s) => Successful(Some(s))
      case IngestCompleted(s) => Successful(Some(s))
      case IngestFailed(s, t, _) => DeterministicFailure(t, Some(s))
      case IngestShouldRetry(s, t, _) => NonDeterministicFailure(t, Some(s))
    }
}
