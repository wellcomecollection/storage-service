package uk.ac.wellcome.platform.archive.bagunpacker.services

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
import uk.ac.wellcome.platform.archive.bagunpacker.builders.BagLocationBuilder
import uk.ac.wellcome.platform.archive.bagunpacker.config.models.BagUnpackerWorkerConfig
import uk.ac.wellcome.platform.archive.bagunpacker.models.UnpackSummary
import uk.ac.wellcome.platform.archive.common.ObjectLocationPayload
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services._
import uk.ac.wellcome.platform.archive.common.storage.models.IngestStepWorker
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

case class BagUnpackerWorker(alpakkaSQSWorkerConfig: AlpakkaSQSWorkerConfig,
                             bagUnpackerWorkerConfig: BagUnpackerWorkerConfig,
                             ingestUpdater: IngestUpdater,
                             outgoingPublisher: OutgoingPublisher,
                             unpacker: Unpacker)(
  implicit actorSystem: ActorSystem,
  ec: ExecutionContext,
  mc: MonitoringClient,
  sc: AmazonSQSAsync)
    extends Runnable
    with Logging
    with IngestStepWorker {
  private val worker: AlpakkaSQSWorker[ObjectLocationPayload, UnpackSummary] =
    AlpakkaSQSWorker[ObjectLocationPayload, UnpackSummary](
      alpakkaSQSWorkerConfig) {
      processMessage
    }

  def processMessage(
    payload: ObjectLocationPayload): Future[Result[UnpackSummary]] = {
    val unpackBagLocation = BagLocationBuilder.build(
      ingestId = payload.ingestId,
      storageSpace = payload.storageSpace,
      unpackerWorkerConfig = bagUnpackerWorkerConfig
    )
    for {
      stepResult <- unpacker.unpack(
        requestId = payload.ingestId.toString,
        srcLocation = payload.objectLocation,
        dstLocation = unpackBagLocation
      )
      _ <- ingestUpdater.send(payload.ingestId, stepResult)
      outgoingPayload = payload.copy(
        objectLocation = unpackBagLocation
      )
      _ <- outgoingPublisher.sendIfSuccessful(stepResult, outgoingPayload)
    } yield toResult(stepResult)
  }

  override def run(): Future[Any] = worker.start

}
