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
import uk.ac.wellcome.platform.archive.common.ObjectLocationPayload
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services.OutgoingPublisher
import uk.ac.wellcome.platform.archive.common.storage.models.IngestStepWorker
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class BagRegisterWorker(
  alpakkaSQSWorkerConfig: AlpakkaSQSWorkerConfig,
  ingestUpdater: IngestUpdater,
  outgoingPublisher: OutgoingPublisher,
  register: Register
)(implicit
  actorSystem: ActorSystem,
  ec: ExecutionContext,
  mc: MonitoringClient,
  sc: AmazonSQSAsync)
    extends Runnable
    with Logging
    with IngestStepWorker {

  private val worker
    : AlpakkaSQSWorker[ObjectLocationPayload, RegistrationSummary] =
    AlpakkaSQSWorker[ObjectLocationPayload, RegistrationSummary](
      alpakkaSQSWorkerConfig) {
      processMessage
    }

  def processMessage(
    payload: ObjectLocationPayload): Future[Result[RegistrationSummary]] =
    for {
      registrationSummary <- register.update(
        bagRootLocation = payload.objectLocation,
        storageSpace = payload.storageSpace
      )
      _ <- ingestUpdater.send(
        payload.ingestId,
        registrationSummary,
        bagId = registrationSummary.summary.bagId)
      _ <- outgoingPublisher.sendIfSuccessful(registrationSummary, payload)
    } yield toResult(registrationSummary)

  override def run(): Future[Any] = worker.start
}
