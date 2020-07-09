package uk.ac.wellcome.platform.archive.bagunpacker.services

import akka.actor.ActorSystem
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.messaging.worker.monitoring.metrics.MetricsMonitoringClient
import uk.ac.wellcome.platform.archive.bagunpacker.builders.BagLocationBuilder
import uk.ac.wellcome.platform.archive.bagunpacker.config.models.BagUnpackerWorkerConfig
import uk.ac.wellcome.platform.archive.bagunpacker.models.UnpackSummary
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services._
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestStepResult,
  IngestStepWorker
}
import uk.ac.wellcome.platform.archive.common.{
  SourceLocationPayload,
  UnpackedBagLocationPayload
}
import uk.ac.wellcome.storage.{S3ObjectLocation, S3ObjectLocationPrefix}

import scala.util.Try

class BagUnpackerWorker[IngestDestination, OutgoingDestination](
  val config: AlpakkaSQSWorkerConfig,
  bagUnpackerWorkerConfig: BagUnpackerWorkerConfig,
  ingestUpdater: IngestUpdater[IngestDestination],
  outgoingPublisher: OutgoingPublisher[OutgoingDestination],
  unpacker: Unpacker[
    S3ObjectLocation,
    S3ObjectLocation,
    S3ObjectLocationPrefix
  ],
  val metricsNamespace: String
)(
  implicit val mc: MetricsMonitoringClient,
  val as: ActorSystem,
  val sc: SqsAsyncClient,
  val wd: Decoder[SourceLocationPayload]
) extends IngestStepWorker[SourceLocationPayload, UnpackSummary[_, _]] {

  def processMessage(
    payload: SourceLocationPayload
  ): Try[IngestStepResult[UnpackSummary[_, _]]] =
    for {
      _ <- ingestUpdater.start(payload.ingestId)

      unpackedBagLocation = BagLocationBuilder.build(
        ingestId = payload.ingestId,
        storageSpace = payload.storageSpace,
        unpackerWorkerConfig = bagUnpackerWorkerConfig
      )

      stepResult <- unpacker.unpack(
        ingestId = payload.ingestId,
        srcLocation =
          payload.sourceLocation.location.asInstanceOf[S3ObjectLocation],
        dstPrefix = unpackedBagLocation
      )

      _ <- ingestUpdater.send(payload.ingestId, stepResult)

      outgoingPayload = UnpackedBagLocationPayload(
        context = payload.context,
        unpackedBagLocation = unpackedBagLocation
      )
      _ <- outgoingPublisher.sendIfSuccessful(stepResult, outgoingPayload)
    } yield stepResult
}
