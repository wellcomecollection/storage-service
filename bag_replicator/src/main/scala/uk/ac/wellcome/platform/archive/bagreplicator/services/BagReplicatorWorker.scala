package uk.ac.wellcome.platform.archive.bagreplicator.services

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import cats.instances.try_._
import io.circe.Decoder
import org.apache.commons.io.IOUtils
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
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.streaming.InputStreamWithLengthAndMetadata
import uk.ac.wellcome.storage.{Identified, ObjectLocation, ObjectLocationPrefix}

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
  val wd: Decoder[VersionedBagRootPayload],
  streamStore: StreamStore[ObjectLocation, InputStreamWithLengthAndMetadata]
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
        payload.storageSpace,
        payload.externalIdentifier,
        payload.version
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

  /** This step is here to check the bag created by the replica and the
    * original bag are the same; the verifier can only check that a
    * bag is correctly formed.
    *
    * Without this check, it would be possible for the replicator to
    * write an entirely different, valid bag -- and because the verifier
    * doesn't have context for the original bag, it wouldn't flag
    * it as an error.
    *
    */
  def checkTagManifestsAreTheSame(
    srcPrefix: ObjectLocationPrefix,
    dstPrefix: ObjectLocationPrefix
  ): Try[Unit] = Try {
    val manifests =
      for {
        srcManifest <- streamStore.get(
          srcPrefix.asLocation("tagmanifest-sha256.txt")
        )
        dstManifest <- streamStore.get(
          dstPrefix.asLocation("tagmanifest-sha256.txt")
        )
      } yield (srcManifest, dstManifest)

    manifests match {
      case Right((Identified(_, srcStream), Identified(_, dstStream))) =>
        if (IOUtils.contentEquals(srcStream, dstStream)) {
          ()
        } else {
          throw new Throwable(
            "tagmanifest-sha256.txt in replica source and replica location do not match!"
          )
        }
      case err =>
        throw new Throwable(
          s"Unable to load tagmanifest-sha256.txt in source and replica to compare: $err"
        )
    }
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
