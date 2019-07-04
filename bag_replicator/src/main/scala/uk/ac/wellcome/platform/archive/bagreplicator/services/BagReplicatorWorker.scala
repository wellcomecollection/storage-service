package uk.ac.wellcome.platform.archive.bagreplicator.services

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import cats.instances.try_._
import com.amazonaws.services.sqs.AmazonSQSAsync
import io.circe.Decoder
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.bagreplicator.config.ReplicatorDestinationConfig
import uk.ac.wellcome.platform.archive.bagreplicator.models.ReplicationSummary
import uk.ac.wellcome.platform.archive.common.EnrichedBagInformationPayload
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services._
import uk.ac.wellcome.platform.archive.common.storage.models._
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.locking.{FailedLockingServiceOp, LockDao, LockingService}

import scala.util.Try

class BagReplicatorWorker[IngestDestination, OutgoingDestination](
  val config: AlpakkaSQSWorkerConfig,
  bagReplicator: BagReplicator,
  ingestUpdater: IngestUpdater[IngestDestination],
  outgoingPublisher: OutgoingPublisher[OutgoingDestination],
  lockingService: LockingService[IngestStepResult[ReplicationSummary],
                                 Try,
                                 LockDao[String, UUID]],
  replicatorDestinationConfig: ReplicatorDestinationConfig
)(implicit
  val mc: MonitoringClient,
  val as: ActorSystem,
  val sc: AmazonSQSAsync,
  val wd: Decoder[EnrichedBagInformationPayload]
) extends IngestStepWorker[EnrichedBagInformationPayload, ReplicationSummary] {
  override val visibilityTimeout = 180

  val destinationBuilder = new DestinationBuilder(
    namespace = replicatorDestinationConfig.namespace,
    rootPath = replicatorDestinationConfig.rootPath
  )

  override def processMessage(
    payload: EnrichedBagInformationPayload,
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
    destination: ObjectLocation): Try[IngestStepResult[ReplicationSummary]] =
    for {
      replicationSummary <- bagReplicator.replicate(
        bagRootLocation = payload.bagRootLocation,
        destination = destination,
        storageSpace = payload.storageSpace
      )
      _ <- ingestUpdater.send(payload.ingestId, replicationSummary)
      _ <- outgoingPublisher.sendIfSuccessful(
        replicationSummary,
        payload.copy(
          bagRootLocation = replicationSummary.summary.destination
        )
      )
    } yield replicationSummary

  def lockFailed(
    payload: EnrichedBagInformationPayload,
    destination: ObjectLocation
  ): PartialFunction[
    Either[FailedLockingServiceOp, IngestStepResult[ReplicationSummary]],
    IngestStepResult[ReplicationSummary]] = {
    case Right(result) => result
    case Left(failedLockingServiceOp) =>
      warn(s"Unable to lock successfully: $failedLockingServiceOp")
      IngestShouldRetry(
        ReplicationSummary(
          bagRootLocation = payload.bagRootLocation,
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
