package uk.ac.wellcome.platform.archive.bag_register.services

import akka.actor.ActorSystem
import com.amazonaws.services.sqs.AmazonSQSAsync
import grizzled.slf4j.Logging
import io.circe.{Decoder, Encoder}
import uk.ac.wellcome.messaging.sqsworker.alpakka.{
  AlpakkaSQSWorker,
  AlpakkaSQSWorkerConfig
}
import uk.ac.wellcome.messaging.worker.models.Result
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.bag_register.models.RegistrationSummary
import uk.ac.wellcome.platform.archive.common.ingests.models.BagRequest
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
  sc: AmazonSQSAsync,
  decoder: Decoder[BagRequest],
  encoder: Encoder[BagRequest])
    extends Runnable
    with Logging
    with IngestStepWorker {

  private val worker: AlpakkaSQSWorker[BagRequest, RegistrationSummary] =
    AlpakkaSQSWorker[BagRequest, RegistrationSummary](alpakkaSQSWorkerConfig) {
      processMessage
    }

  def processMessage(
    bagRequest: BagRequest): Future[Result[RegistrationSummary]] =
    for {
      registrationSummary <- register.update(bagRequest.bagLocation)
      _ <- ingestUpdater.send(
        bagRequest.ingestId,
        registrationSummary,
        bagId = registrationSummary.summary.bagId)
      _ <- outgoingPublisher.sendIfSuccessful(registrationSummary, bagRequest)
    } yield toResult(registrationSummary)

  override def run(): Future[Any] = worker.start
}
