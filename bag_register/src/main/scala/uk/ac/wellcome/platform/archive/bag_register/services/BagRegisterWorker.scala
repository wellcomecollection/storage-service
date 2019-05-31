package uk.ac.wellcome.platform.archive.bag_register.services

import akka.actor.ActorSystem
import com.amazonaws.services.sqs.AmazonSQSAsync
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sqsworker.alpakka.{
  AlpakkaSQSWorker,
  AlpakkaSQSWorkerConfig
}
import uk.ac.wellcome.messaging.worker.models.Result
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.bag_register.models.RegistrationSummary
import uk.ac.wellcome.platform.archive.common.BagInformationPayload
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services.OutgoingPublisher
import uk.ac.wellcome.platform.archive.common.storage.models.IngestStepWorker
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.Future
import scala.util.Try

class BagRegisterWorker[IngestDestination, OutgoingDestination](
  workerConfig: AlpakkaSQSWorkerConfig,
  ingestUpdater: IngestUpdater[IngestDestination],
  outgoingPublisher: OutgoingPublisher[OutgoingDestination],
  register: Register
)(implicit
  actorSystem: ActorSystem,
  mc: MonitoringClient,
  sc: AmazonSQSAsync)
    extends Runnable
    with Logging
    with IngestStepWorker {

  private val worker =
    AlpakkaSQSWorker[BagInformationPayload, RegistrationSummary](
      workerConfig) { payload => Future.fromTry(processMessage(payload)) }

  def processMessage(
    payload: BagInformationPayload): Try[Result[RegistrationSummary]] =
    for {
      _ <- ingestUpdater.start(payload.ingestId)

      registrationSummary <- register.update(
        bagRootLocation = payload.bagRootLocation,
        version = payload.version,
        storageSpace = payload.storageSpace
      )

      _ <- ingestUpdater.send(
        ingestId = payload.ingestId,
        step = registrationSummary,
        bagId = registrationSummary.summary.bagId
      )

      _ <- outgoingPublisher.sendIfSuccessful(
        result = registrationSummary,
        outgoing = payload
      )

    } yield toResult(registrationSummary)

  override def run(): Future[Any] = worker.start
}
