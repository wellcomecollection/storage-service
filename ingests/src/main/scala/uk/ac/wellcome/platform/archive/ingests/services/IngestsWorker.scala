package uk.ac.wellcome.platform.archive.ingests.services

import akka.actor.ActorSystem
import com.amazonaws.services.sqs.AmazonSQSAsync
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.MessageSender
import uk.ac.wellcome.messaging.sqsworker.alpakka.{
  AlpakkaSQSWorker,
  AlpakkaSQSWorkerConfig
}
import uk.ac.wellcome.messaging.worker.models.{
  NonDeterministicFailure,
  Result,
  Successful
}
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Ingest,
  IngestUpdate
}
import uk.ac.wellcome.platform.archive.common.ingests.tracker.IngestTracker
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class IngestsWorker[CallbackDestination, UpdatedIngestsDestination](
  alpakkaSQSWorkerConfig: AlpakkaSQSWorkerConfig,
  ingestTracker: IngestTracker,
  callbackNotificationService: CallbackNotificationService[CallbackDestination],
  updatedIngestsMessageSender: MessageSender[UpdatedIngestsDestination]
)(implicit actorSystem: ActorSystem, mc: MonitoringClient, sc: AmazonSQSAsync)
    extends Runnable
    with Logging {

  private val worker =
    AlpakkaSQSWorker[IngestUpdate, Ingest](alpakkaSQSWorkerConfig) { payload =>
      Future.fromTry { processMessage(payload) }
    }

  def processMessage(ingestUpdate: IngestUpdate): Try[Result[Ingest]] = {
    val result = for {
      ingest <- ingestTracker.update(ingestUpdate) match {
        case Right(updatedIngest) => Success(updatedIngest.identifiedT)
        case Left(err) =>
          Failure(
            new Throwable(s"Error from the ingest tracker: $err")
          )
      }
      _ <- sendOngoingMessages(ingest)
    } yield ingest

    result match {
      case Success(ingest) => Success(Successful(Some(ingest)))

      // It's possible for an ingest update to fail in a flaky way
      // (e.g. a DynamoDB conditional update failure).
      //
      // Rather than trying to distinguish these, we just send all
      // failed updates back onto the queue and log the reason.
      case Failure(err) =>
        warn(s"Error trying to apply update $ingestUpdate: $err")
        Success(NonDeterministicFailure(err, summary = None))
    }
  }

  // The ingests monitor needs to send up to two messages:
  //
  //  - if the ingest is complete, it sends a message to the notifier to trigger
  //    a callback to the user
  //  - it sends the updated ingest to an SNS topic
  //
  // These two messages are independent, and so can succeed/fail independently.
  //
  private def sendOngoingMessages(ingest: Ingest): Try[Unit] = {
    val callbackResult = callbackNotificationService.sendNotification(ingest)
    val updatedIngestResult = updatedIngestsMessageSender.sendT(ingest)

    (callbackResult, updatedIngestResult) match {
      case (Success(_), Success(_))   => Success(())

      case (Failure(callbackErr), Success(_)) =>
        warn(s"Failed to send the callback notification: $callbackErr")
        Failure(callbackErr)

      case (Success(_), Failure(updatedIngestErr)) =>
        warn(s"Failed to send the updated ingest: $updatedIngestErr")
        Failure(updatedIngestErr)

      case (Failure(callbackErr), Failure(updatedIngestErr)) =>
        warn(s"Failed to send the callback notification: $callbackErr")
        warn(s"Failed to send the updated ingest: $updatedIngestErr")
        Failure(new Throwable("Both of the ongoing messages failed to send correctly!"))
    }
  }

  override def run(): Future[Any] = worker.start
}
