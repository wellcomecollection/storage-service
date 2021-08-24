package weco.storage_service.bag_replicator.services

import java.time.Instant
import java.util.UUID
import akka.actor.ActorSystem
import cats.instances.try_._
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import weco.monitoring.Metrics
import weco.storage_service.bag_replicator.config.ReplicatorDestinationConfig
import weco.storage_service.bag_replicator.replicator.Replicator
import weco.storage_service.bag_replicator.replicator.models._
import weco.storage_service.ingests.models.IngestID
import weco.storage_service.ingests.services.IngestUpdater
import weco.storage_service.operation.services._
import weco.storage_service.storage.models._
import weco.storage_service.{ReplicaCompletePayload, VersionedBagRootPayload}
import weco.storage.{Location, Prefix}
import weco.storage.locking.{FailedLockingServiceOp, LockDao, LockingService}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Try

class BagReplicatorWorker[
  IngestDestination,
  OutgoingDestination,
  SrcLocation,
  DstLocation <: Location,
  DstPrefix <: Prefix[DstLocation]
](
  val config: AlpakkaSQSWorkerConfig,
  ingestUpdater: IngestUpdater[IngestDestination],
  outgoingPublisher: OutgoingPublisher[OutgoingDestination],
  lockingService: LockingService[IngestStepResult[
    ReplicationSummary[DstPrefix]
  ], Try, LockDao[
    String,
    UUID
  ]],
  destinationConfig: ReplicatorDestinationConfig,
  replicator: Replicator[SrcLocation, DstLocation, DstPrefix],
  val metricsNamespace: String,
  override val visibilityTimeout: Duration = 3.minutes
)(
  implicit
  val mc: Metrics[Future],
  val as: ActorSystem,
  val sc: SqsAsyncClient,
  val wd: Decoder[VersionedBagRootPayload]
) extends IngestStepWorker[VersionedBagRootPayload, ReplicationSummary[
      DstPrefix
    ]] {

  def processMessage(
    payload: VersionedBagRootPayload
  ): Try[IngestStepResult[ReplicationSummary[DstPrefix]]] =
    for {
      _ <- ingestUpdater.start(payload.ingestId)

      srcPrefix = payload.bagRoot

      dstPrefix = replicator.buildDestination(
        namespace = destinationConfig.namespace,
        space = payload.storageSpace,
        externalIdentifier = payload.externalIdentifier,
        version = payload.version
      )

      replicationRequest = ReplicationRequest(
        srcPrefix = srcPrefix,
        dstPrefix = dstPrefix
      )

      result <- lockingService
        .withLock(dstPrefix.toString) {
          replicate(payload.ingestId, replicationRequest)
        }
        .map(lockFailed(payload.ingestId, replicationRequest).apply(_))

      _ <- ingestUpdater.send(payload.ingestId, result)

      _ <- outgoingPublisher.sendIfSuccessful(
        result,
        ReplicaCompletePayload(
          context = payload.context,
          srcPrefix = replicationRequest.srcPrefix,
          dstLocation = replicationRequest
            .toReplicaLocation(
              replicaType = destinationConfig.replicaType
            ),
          version = payload.version
        )
      )
    } yield result

  def replicate(
    ingestId: IngestID,
    request: ReplicationRequest[DstPrefix]
  ): Try[IngestStepResult[ReplicationSummary[DstPrefix]]] = Try {
    replicator.replicate(
      ingestId = ingestId,
      request = request
    ) match {
      case ReplicationSucceeded(summary) => IngestStepSucceeded(summary)
      case ReplicationFailed(summary, e) => IngestFailed(summary, e)
    }
  }

  def lockFailed(
    ingestId: IngestID,
    request: ReplicationRequest[DstPrefix]
  ): PartialFunction[Either[FailedLockingServiceOp, IngestStepResult[
    ReplicationSummary[DstPrefix]
  ]], IngestStepResult[ReplicationSummary[DstPrefix]]] = {
    case Right(result) => result
    case Left(failedLockingServiceOp) =>
      warn(s"Unable to lock successfully: $failedLockingServiceOp")
      IngestShouldRetry(
        ReplicationSummary(
          ingestId = ingestId,
          startTime = Instant.now,
          request = request
        ),
        new Throwable(
          s"Unable to lock successfully: $failedLockingServiceOp"
        )
      )
  }
}
