package uk.ac.wellcome.platform.archive.bagunpacker.services

import akka.actor.ActorSystem
import com.amazonaws.services.sqs.AmazonSQSAsync
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.bagunpacker.builders.BagLocationBuilder
import uk.ac.wellcome.platform.archive.bagunpacker.config.models.BagUnpackerWorkerConfig
import uk.ac.wellcome.platform.archive.bagunpacker.models.UnpackSummary
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services._
import uk.ac.wellcome.platform.archive.common.storage.models.{IngestStepResult, IngestStepWorker}
import uk.ac.wellcome.platform.archive.common.{SourceLocationPayload, UnpackedBagLocationPayload}

import scala.util.Try

class BagUnpackerWorker[IngestDestination, OutgoingDestination](
  val config: AlpakkaSQSWorkerConfig,
  bagUnpackerWorkerConfig: BagUnpackerWorkerConfig,
  ingestUpdater: IngestUpdater[IngestDestination],
  outgoingPublisher: OutgoingPublisher[OutgoingDestination],
  unpacker: Unpacker)(implicit
                      actorSystem: ActorSystem,
                      mc: MonitoringClient,
                      sc: AmazonSQSAsync)
    extends IngestStepWorker[
      SourceLocationPayload,
      UnpackSummary] {

  def processMessage(
    payload: SourceLocationPayload): Try[IngestStepResult[UnpackSummary]] =
    for {
      _ <- ingestUpdater.start(payload.ingestId)

      unpackedBagLocation = BagLocationBuilder.build(
        ingestId = payload.ingestId,
        storageSpace = payload.storageSpace,
        unpackerWorkerConfig = bagUnpackerWorkerConfig
      )

      stepResult <- unpacker.unpack(
        requestId = payload.ingestId.toString,
        srcLocation = payload.sourceLocation,
        dstLocation = unpackedBagLocation
      )

      _ <- ingestUpdater.send(payload.ingestId, stepResult)

      outgoingPayload = UnpackedBagLocationPayload(
        context = payload.context,
        unpackedBagLocation = unpackedBagLocation
      )
      _ <- outgoingPublisher.sendIfSuccessful(stepResult, outgoingPayload)
    } yield stepResult
}
