package uk.ac.wellcome.platform.archive.bagreplicator.services

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import cats.instances.try_._
import com.amazonaws.services.sqs.AmazonSQSAsync
import io.circe.Decoder
import org.apache.commons.io.IOUtils
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.bagreplicator.config.ReplicatorDestinationConfig
import uk.ac.wellcome.platform.archive.bagreplicator.models.ReplicationSummary
import uk.ac.wellcome.platform.archive.common.EnrichedBagInformationPayload
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services._
import uk.ac.wellcome.platform.archive.common.storage.models._
import uk.ac.wellcome.storage.locking.{
  FailedLockingServiceOp,
  LockDao,
  LockingService
}
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.streaming.InputStreamWithLengthAndMetadata
import uk.ac.wellcome.storage.{Identified, ObjectLocation, ObjectLocationPrefix}

import scala.util.{Failure, Success, Try}

class BagReplicatorWorker[IngestDestination, OutgoingDestination](
  val config: AlpakkaSQSWorkerConfig,
  bagReplicator: BagReplicator,
  ingestUpdater: IngestUpdater[IngestDestination],
  outgoingPublisher: OutgoingPublisher[OutgoingDestination],
  lockingService: LockingService[IngestStepResult[ReplicationSummary], Try, LockDao[
    String,
    UUID
  ]],
  replicatorDestinationConfig: ReplicatorDestinationConfig
)(
  implicit
  val mc: MonitoringClient,
  val as: ActorSystem,
  val sc: AmazonSQSAsync,
  val wd: Decoder[EnrichedBagInformationPayload],
  streamStore: StreamStore[ObjectLocation, InputStreamWithLengthAndMetadata]
) extends IngestStepWorker[EnrichedBagInformationPayload, ReplicationSummary] {
  override val visibilityTimeout = 180

  val destinationBuilder = new DestinationBuilder(
    namespace = replicatorDestinationConfig.namespace,
    rootPath = replicatorDestinationConfig.rootPath
  )

  override def processMessage(
    payload: EnrichedBagInformationPayload
  ): Try[IngestStepResult[ReplicationSummary]] =
    for {
      _ <- ingestUpdater.start(payload.ingestId)

      destination = destinationBuilder.buildDestination(
        storageSpace = payload.storageSpace,
        externalIdentifier = payload.externalIdentifier,
        version = payload.version
      )

      result <- lockingService
        .withLock(payload.ingestId.toString) {
          replicate(payload, destination)
        }
        .map(lockFailed(payload, destination).apply(_))

    } yield result

  def replicate(
    payload: EnrichedBagInformationPayload,
    destination: ObjectLocationPrefix
  ): Try[IngestStepResult[ReplicationSummary]] = {
    val src = payload.bagRootLocation.asPrefix

    for {
      ingestStep: IngestStepResult[ReplicationSummary] <- bagReplicator.replicate(
        bagRootLocation = src,
        destination = destination,
        storageSpace = payload.storageSpace
      )

      result <- checkTagManifestsAreTheSame(src, destination) match {
        case Success(_) => Success(ingestStep)
        case Failure(err) =>
          Success(
            IngestFailed(
              summary = ingestStep.summary,
              e = err
            )
          )
      }

      _ <- ingestUpdater.send(payload.ingestId, result)
      _ <- outgoingPublisher.sendIfSuccessful(
        result,
        payload.copy(
          bagRootLocation = ingestStep.summary.destination.asLocation()
        )
      )
    } yield result
  }

  /** This step is here to check the bag created by the replica and the
    * original bag are the same; the verifier can only check that a
    * bag is correctly formed.
    *
    * Without this check, it would be possible for the replicator to
    * write an entirely different, valid bag -- and the pipeline would
    * never notice.
    *
    */
  def checkTagManifestsAreTheSame(
    src: ObjectLocationPrefix,
    dst: ObjectLocationPrefix
  ): Try[Unit] = Try {
    val manifests =
      for {
        srcManifest <- streamStore.get(src.asLocation("tagmanifest-sha256.txt"))
        dstManifest <- streamStore.get(dst.asLocation("tagmanifest-sha256.txt"))
      } yield (srcManifest, dstManifest)

    manifests match {
      case Right((Identified(_, srcStream), Identified(_, dstStream))) =>
        if (IOUtils.contentEquals(srcStream, dstStream)) {
          ()
        } else {
          throw new Throwable("tagmanifest-sha256.txt in replica source and replica location do not match!")
        }
      case err =>
        throw new Throwable(s"Unable to load tagmanifest-sha256.txt in source and replica to compare: $err")
    }
  }


  def lockFailed(
    payload: EnrichedBagInformationPayload,
    destination: ObjectLocationPrefix
  ): PartialFunction[Either[FailedLockingServiceOp, IngestStepResult[
    ReplicationSummary
  ]], IngestStepResult[ReplicationSummary]] = {
    case Right(result) => result
    case Left(failedLockingServiceOp) =>
      warn(s"Unable to lock successfully: $failedLockingServiceOp")
      IngestShouldRetry(
        ReplicationSummary(
          bagRootLocation = payload.bagRootLocation.asPrefix,
          storageSpace = payload.storageSpace,
          destination = destination,
          startTime = Instant.now
        ),
        new Throwable(
          s"Unable to lock successfully: $failedLockingServiceOp"
        )
      )
  }
}
