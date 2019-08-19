package uk.ac.wellcome.platform.archive.common.worker

import akka.actor.ActorSystem
import akka.stream.alpakka.sqs
import akka.stream.alpakka.sqs.MessageAction
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model.Message
import grizzled.slf4j.Logging
import io.circe.Decoder
import uk.ac.wellcome.messaging.sqsworker.alpakka.{AlpakkaSQSWorker, AlpakkaSQSWorkerConfig}
import uk.ac.wellcome.messaging.worker.models.{DeterministicFailure, NonDeterministicFailure, Result, Successful}
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.common.PipelinePayload
import uk.ac.wellcome.platform.archive.common.storage.models.{IngestCompleted, IngestFailed, IngestShouldRetry, IngestStepResult, IngestStepSucceeded}
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.Future
import scala.util.Try

trait StepWorker[Work, Summary]
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
      case IngestStepSucceeded(s, _)  => Successful(Some(s))
      case IngestCompleted(s)         => Successful(Some(s))
      case IngestFailed(s, t, _)      => DeterministicFailure(t, Some(s))
      case IngestShouldRetry(s, t, _) => NonDeterministicFailure(t, Some(s))
    }
}

