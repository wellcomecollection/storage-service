package uk.ac.wellcome.platform.archive.bagreplicator.services

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import cats.instances.try_._
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.messaging.worker.monitoring.metrics.MetricsMonitoringClient
import uk.ac.wellcome.platform.archive.bagreplicator.bags.BagReplicator
import uk.ac.wellcome.platform.archive.bagreplicator.bags.models._
import uk.ac.wellcome.platform.archive.bagreplicator.config.ReplicatorDestinationConfig
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.models.ReplicationRequest
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services._
import uk.ac.wellcome.platform.archive.common.storage.models._
import uk.ac.wellcome.platform.archive.common.storage.services.DestinationBuilder
import uk.ac.wellcome.platform.archive.common.{
  ReplicaResultPayload,
  VersionedBagRootPayload
}
import uk.ac.wellcome.storage.locking.{
  FailedLockingServiceOp,
  LockDao,
  LockingService
}

import scala.util.Try

class BagReplicatorWorker[
  IngestDestination,
  OutgoingDestination
](
  val config: AlpakkaSQSWorkerConfig,
  ingestUpdater: IngestUpdater[IngestDestination],
  outgoingPublisher: OutgoingPublisher[OutgoingDestination],
  lockingService: LockingService[IngestStepResult[BagReplicationSummary[_]], Try, LockDao[
    String,
    UUID
  ]],
  destinationConfig: ReplicatorDestinationConfig,
  bagReplicator: BagReplicator,
  val metricsNamespace: String
)(
  implicit
  val mc: MetricsMonitoringClient,
  val as: ActorSystem,
  val sc: SqsAsyncClient,
  val wd: Decoder[VersionedBagRootPayload]
) extends IngestStepWorker[
      VersionedBagRootPayload,
      BagReplicationSummary[_]
    ] {
  override val visibilityTimeout = 180

  def processMessage(
    payload: VersionedBagRootPayload
  ): Try[IngestStepResult[BagReplicationSummary[_]]] =
    for {
      _ <- ingestUpdater.start(payload.ingestId)

      srcPrefix = payload.bagRoot

      dstPrefix = DestinationBuilder.buildDestination(
        namespace = destinationConfig.namespace,
        storageSpace = payload.storageSpace,
        externalIdentifier = payload.externalIdentifier,
        version = payload.version
      )

      replicationRequest = destinationConfig.requestBuilder(
        ReplicationRequest(
          srcPrefix = srcPrefix,
          dstPrefix = dstPrefix
        )
      )

      result <- lockingService
        .withLock(dstPrefix.toString) {
          replicate(payload.ingestId, replicationRequest)
        }
        .map(lockFailed(payload.ingestId, replicationRequest).apply(_))

      _ <- ingestUpdater.send(payload.ingestId, result)

      _ <- outgoingPublisher.sendIfSuccessful(
        result,
        ReplicaResultPayload(
          context = payload.context,
          version = payload.version,
          replicaResult =
            replicationRequest.toResult(destinationConfig.provider)
        )
      )
    } yield result

  def replicate(
    ingestId: IngestID,
    bagReplicationRequest: BagReplicationRequest
  ): Try[IngestStepResult[BagReplicationSummary[BagReplicationRequest]]] =
    bagReplicator
      .replicateBag(
        ingestId = ingestId,
        bagRequest = bagReplicationRequest
      )
      .map {
        case BagReplicationSucceeded(summary) =>
          IngestStepSucceeded(summary)

        case BagReplicationFailed(summary, e) =>
          IngestFailed(summary, e)
      }

  def lockFailed(
    ingestId: IngestID,
    request: BagReplicationRequest
  ): PartialFunction[Either[FailedLockingServiceOp, IngestStepResult[
    BagReplicationSummary[_]
  ]], IngestStepResult[BagReplicationSummary[_]]] = {
    case Right(result) => result
    case Left(failedLockingServiceOp) =>
      warn(s"Unable to lock successfully: $failedLockingServiceOp")
      IngestShouldRetry(
        BagReplicationSummary(
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
