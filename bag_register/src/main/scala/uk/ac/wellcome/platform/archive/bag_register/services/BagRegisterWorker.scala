package uk.ac.wellcome.platform.archive.bag_register.services

import akka.actor.ActorSystem
import com.amazonaws.services.sqs.AmazonSQSAsync
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.bag_register.models.RegistrationSummary
import uk.ac.wellcome.platform.archive.common.EnrichedBagInformationPayload
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services.OutgoingPublisher
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestStepResult,
  IngestStepWorker
}

import scala.util.Try

class BagRegisterWorker[IngestDestination, OutgoingDestination](
  val config: AlpakkaSQSWorkerConfig,
  ingestUpdater: IngestUpdater[IngestDestination],
  outgoingPublisher: OutgoingPublisher[OutgoingDestination],
  register: Register
)(implicit
  actorSystem: ActorSystem,
  mc: MonitoringClient,
  sc: AmazonSQSAsync)
    extends IngestStepWorker[EnrichedBagInformationPayload, RegistrationSummary] {

  override def processMessage(payload: EnrichedBagInformationPayload)
    : Try[IngestStepResult[RegistrationSummary]] =
    for {
      _ <- ingestUpdater.start(payload.ingestId)

      registrationSummary <- register.update(
        bagRootLocation = payload.bagRootLocation,
        version = payload.version,
        storageSpace = payload.storageSpace
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
