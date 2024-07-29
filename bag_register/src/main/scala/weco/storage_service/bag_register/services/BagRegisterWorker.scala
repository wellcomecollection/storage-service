package weco.storage_service.bag_register.services

import org.apache.pekko.actor.ActorSystem
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.json.JsonUtil._
import weco.messaging.MessageSender
import weco.messaging.sqsworker.pekko.PekkoSQSWorkerConfig
import weco.messaging.worker.models.Result
import weco.monitoring.Metrics
import weco.storage_service.bag_register.models.RegistrationSummary
import weco.storage_service.{BagRegistrationNotification, KnownReplicasPayload}
import weco.storage_service.ingests.services.IngestUpdater
import weco.storage_service.storage.models.{
  IngestCompleted,
  IngestStepResult,
  IngestStepWorker
}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class BagRegisterWorker[IngestDestination, NotificationDestination](
  val config: PekkoSQSWorkerConfig,
  ingestUpdater: IngestUpdater[IngestDestination],
  registrationNotifications: MessageSender[NotificationDestination],
  register: Register
)(
  implicit
  val mc: Metrics[Future],
  val as: ActorSystem,
  val sc: SqsAsyncClient,
  val wd: Decoder[KnownReplicasPayload]
) extends IngestStepWorker[KnownReplicasPayload, RegistrationSummary] {
  implicit val ec: ExecutionContext = as.dispatcher

  // The bag register can fail if the bag tracker isn't available.  Rather than
  // retrying immediately, allow a short delay before retrying a registration.
  //
  // Registration isn't a time-critical process, so a delay is acceptable.
  override val visibilityTimeout: Duration = 2.minutes

  override def process(
    payload: KnownReplicasPayload
  ): Future[Result[RegistrationSummary]] =
    processPayload(payload).map { toResult }

  def processPayload(
    payload: KnownReplicasPayload
  ): Future[IngestStepResult[RegistrationSummary]] =
    for {
      _ <- Future.fromTry {
        ingestUpdater.start(payload.ingestId)
      }

      registrationSummary <- register.update(
        ingestId = payload.ingestId,
        location = payload.knownReplicas.location,
        replicas = payload.knownReplicas.replicas,
        version = payload.version,
        space = payload.storageSpace,
        externalIdentifier = payload.externalIdentifier
      )

      _ <- Future.fromTry {
        ingestUpdater.send(
          ingestId = payload.ingestId,
          step = registrationSummary
        )
      }

      _ <- Future.fromTry {
        sendRegistrationNotification(registrationSummary)
      }
    } yield registrationSummary

  private def sendRegistrationNotification(
    result: IngestStepResult[RegistrationSummary]
  ): Try[Unit] =
    result match {
      case IngestCompleted(summary) =>
        registrationNotifications.sendT[BagRegistrationNotification](
          BagRegistrationNotification(
            space = summary.space,
            externalIdentifier = summary.externalIdentifier,
            version = summary.version.toString
          )
        )

      case _ => Success(())
    }

  // The IngestStepWorker trait expects a processMessage() method, which returns
  // a Try[…].  That method then gets called to provide the process() method,
  // which is in turn used by the PekkoSQSWorker.
  //
  // Because the bag tracker client returns a Future[…] rather than a Try[…],
  // we bypass this method and define our own process().  We still have to define
  // a method here, but it should never be called.
  override def processMessage(
    payload: KnownReplicasPayload
  ): Try[IngestStepResult[RegistrationSummary]] =
    Failure(new Throwable("Not used"))
}
