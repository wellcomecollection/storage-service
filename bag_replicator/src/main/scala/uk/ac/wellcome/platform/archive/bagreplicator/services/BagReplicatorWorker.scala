package uk.ac.wellcome.platform.archive.bagreplicator.services

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import cats.instances.future._
import com.amazonaws.services.sqs.AmazonSQSAsync
import io.circe.Decoder
import org.apache.commons.io.IOUtils
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.messaging.worker.models.Result
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.bagreplicator.bags.BagReplicator
import uk.ac.wellcome.platform.archive.bagreplicator.bags.models._
import uk.ac.wellcome.platform.archive.bagreplicator.config.ReplicatorDestinationConfig
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.models.ReplicationRequest
import uk.ac.wellcome.platform.archive.common.EnrichedBagInformationPayload
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services._
import uk.ac.wellcome.platform.archive.common.storage.models._
import uk.ac.wellcome.storage.locking.{FailedLockingServiceOp, LockDao, LockingService}
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.streaming.InputStreamWithLengthAndMetadata
import uk.ac.wellcome.storage.{Identified, ObjectLocation, ObjectLocationPrefix}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Try}

class BagReplicatorWorker[BagRequest <: BagReplicationRequest, IngestDestination, OutgoingDestination](
  val config: AlpakkaSQSWorkerConfig,
  ingestUpdater: IngestUpdater[IngestDestination],
  outgoingPublisher: OutgoingPublisher[OutgoingDestination],
  lockingService: LockingService[IngestStepResult[BagReplicationSummary[_]], Future, LockDao[
    String,
    UUID
  ]],
  replicatorDestinationConfig: ReplicatorDestinationConfig,
  bagReplicator: BagReplicator[BagRequest],
  createBagRequest: ReplicationRequest => BagRequest
)(
  implicit
  val mc: MonitoringClient,
  val as: ActorSystem,
  val sc: AmazonSQSAsync,
  val wd: Decoder[EnrichedBagInformationPayload],
  ec: ExecutionContext,
  streamStore: StreamStore[ObjectLocation, InputStreamWithLengthAndMetadata]
) extends IngestStepWorker[EnrichedBagInformationPayload, BagReplicationSummary[_]] {
  override val visibilityTimeout = 180

  val destinationBuilder = new DestinationBuilder(
    namespace = replicatorDestinationConfig.namespace
  )

//  val bagReplicator = new BagReplicator[BagRequest](replicator)

  override def process(
    payload: EnrichedBagInformationPayload
  ): Future[Result[BagReplicationSummary[_]]] =
    processPayload(payload).map { toResult }

  // The base trait assumes that processMessage will always return
  // a Try; in this case we return a Future, so we override process()
  // above and expect that this will never be used.
  override def processMessage(
    payload: EnrichedBagInformationPayload
  ): Try[IngestStepResult[BagReplicationSummary[_]]] =
    Failure(new Throwable("This should never be called!"))

  def processPayload(
    payload: EnrichedBagInformationPayload
  ): Future[IngestStepResult[BagReplicationSummary[_]]] =
    for {
      _ <- Future.fromTry {
        ingestUpdater.start(payload.ingestId)
      }

      srcPrefix = payload.bagRootLocation.asPrefix

      dstPrefix = destinationBuilder.buildDestination(
        storageSpace = payload.storageSpace,
        externalIdentifier = payload.externalIdentifier,
        version = payload.version
      )

      bagRequest = createBagRequest(
        ReplicationRequest(
          srcPrefix = srcPrefix,
          dstPrefix = dstPrefix
        )
      )

      result <- lockingService
        .withLock(payload.ingestId.toString) {
          replicate(payload, bagRequest)
        }
        .map(lockFailed(bagRequest).apply(_))

      _ <- Future.fromTry {
        ingestUpdater.send(payload.ingestId, result)
      }

      _ <- Future.fromTry {
        outgoingPublisher.sendIfSuccessful(
          result,
          payload.copy(
            bagRootLocation = dstPrefix.asLocation()
          )
        )
      }
    } yield result

  def replicate(
    payload: EnrichedBagInformationPayload,
    bagReplicationRequest: BagRequest
  ): Future[IngestStepResult[BagReplicationSummary[_]]] = {
    val future: Future[BagReplicationResult[_]] =
      bagReplicator.replicateBag(bagReplicationRequest)

    future.map {
      case BagReplicationSucceeded(summary) =>
        IngestStepSucceeded(summary)

      case BagReplicationFailed(summary, e) =>
        IngestFailed(summary, e)
    }
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
    request: BagReplicationRequest
  ): PartialFunction[Either[FailedLockingServiceOp, IngestStepResult[
    BagReplicationSummary[_]
  ]], IngestStepResult[BagReplicationSummary[_]]] = {
    case Right(result) => result
    case Left(failedLockingServiceOp) =>
      warn(s"Unable to lock successfully: $failedLockingServiceOp")
      IngestShouldRetry(
        BagReplicationSummary(
          startTime = Instant.now,
          request = request
        ),
        new Throwable(
          s"Unable to lock successfully: $failedLockingServiceOp"
        )
      )
  }
}
