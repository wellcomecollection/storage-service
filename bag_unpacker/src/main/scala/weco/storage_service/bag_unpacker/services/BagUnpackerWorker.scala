package weco.storage_service.bag_unpacker.services

import org.apache.pekko.actor.ActorSystem
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.messaging.sqsworker.pekko.PekkoSQSWorkerConfig
import weco.monitoring.Metrics
import weco.storage_service.bag_unpacker.builders.BagLocationBuilder
import weco.storage_service.bag_unpacker.config.models.BagUnpackerWorkerConfig
import weco.storage_service.bag_unpacker.models.UnpackSummary
import weco.storage_service.ingests.services.IngestUpdater
import weco.storage_service.operation.services._
import weco.storage_service.storage.models.{IngestStepResult, IngestStepWorker}
import weco.storage_service.{SourceLocationPayload, UnpackedBagLocationPayload}
import weco.storage.providers.s3.{S3ObjectLocation, S3ObjectLocationPrefix}

import scala.concurrent.Future
import scala.util.Try

class BagUnpackerWorker[IngestDestination, OutgoingDestination](
  val config: PekkoSQSWorkerConfig,
  bagUnpackerWorkerConfig: BagUnpackerWorkerConfig,
  ingestUpdater: IngestUpdater[IngestDestination],
  outgoingPublisher: OutgoingPublisher[OutgoingDestination],
  unpacker: Unpacker[
    S3ObjectLocation,
    S3ObjectLocation,
    S3ObjectLocationPrefix
  ]
)(
  implicit val mc: Metrics[Future],
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
