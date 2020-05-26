package uk.ac.wellcome.platform.archive.bag_register.services

import akka.actor.ActorSystem
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.messaging.worker.models.Result
import uk.ac.wellcome.messaging.worker.monitoring.metrics.MetricsMonitoringClient
import uk.ac.wellcome.platform.archive.bag_register.models.RegistrationSummary
import uk.ac.wellcome.platform.archive.common.KnownReplicasPayload
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services.OutgoingPublisher
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestStepResult,
  IngestStepWorker
}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Try}

class BagRegisterWorker[IngestDestination, OutgoingDestination](
  val config: AlpakkaSQSWorkerConfig,
  ingestUpdater: IngestUpdater[IngestDestination],
  outgoingPublisher: OutgoingPublisher[OutgoingDestination],
  register: Register,
  val metricsNamespace: String
)(
  implicit
  val mc: MetricsMonitoringClient,
  val as: ActorSystem,
  val sc: SqsAsyncClient,
  val wd: Decoder[KnownReplicasPayload]
) extends IngestStepWorker[KnownReplicasPayload, RegistrationSummary] {
  implicit val ec: ExecutionContext = as.dispatcher

  override def process(payload: KnownReplicasPayload): Future[Result[RegistrationSummary]] =
    processPayload(payload).map { toResult }

  def processPayload(payload: KnownReplicasPayload): Future[IngestStepResult[RegistrationSummary]] =
    for {
      _ <- Future.fromTry {
        ingestUpdater.start(payload.ingestId)
      }

      registrationSummary <- register.update(
        ingestId = payload.ingestId,
        location = payload.knownReplicas.location,
        replicas = payload.knownReplicas.replicas,
        version = payload.version,
        space = payload.storageSpace
      )

      _ <- Future.fromTry {
        ingestUpdater.send(
          ingestId = payload.ingestId,
          step = registrationSummary
        )
      }

      _ <- Future.fromTry {
        outgoingPublisher.sendIfSuccessful(
          result = registrationSummary,
          outgoing = payload
        )
      }
    } yield registrationSummary

  // The IngestStepWorker trait expects a processMessage() method, which returns
  // a Try[…].  That method then gets called to provide the process() method,
  // which is in turn used by the AlpakkaSQSWorker.
  //
  // Because the bag tracker client returns a Future[…] rather than a Try[…],
  // we bypass this method and define our own process().  We still have to define
  // a method here, but it should never be called.
  override def processMessage(payload: KnownReplicasPayload): Try[IngestStepResult[RegistrationSummary]] =
    Failure(new Throwable("Not used"))
}
