package uk.ac.wellcome.platform.archive.bag_register.services

import akka.actor.ActorSystem
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.messaging.worker.monitoring.metrics.MetricsMonitoringClient
import uk.ac.wellcome.platform.archive.bag_register.models.RegistrationSummary
import uk.ac.wellcome.platform.archive.common.KnownReplicasPayload
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services.OutgoingPublisher
import uk.ac.wellcome.platform.archive.common.storage.models.{IngestStepResult, IngestStepWorker}

import scala.util.Try

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

  override def processMessage(
    payload: KnownReplicasPayload
  ): Try[IngestStepResult[RegistrationSummary]] =
    for {
      _ <- ingestUpdater.start(payload.ingestId)

      registrationSummary <- register.update(
        ingestId = payload.ingestId,
        location = payload.knownReplicas.location,
        replicas = payload.knownReplicas.replicas,
        version = payload.version,
        space = payload.storageSpace
      )

      _ <- ingestUpdater.send(
        ingestId = payload.ingestId,
        step = registrationSummary
      )

      _ <- outgoingPublisher.sendIfSuccessful(
        result = registrationSummary,
        outgoing = payload
      )

    } yield registrationSummary
}
