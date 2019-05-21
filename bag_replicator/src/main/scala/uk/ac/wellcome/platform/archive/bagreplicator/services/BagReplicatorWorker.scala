package uk.ac.wellcome.platform.archive.bagreplicator.services

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.alpakka.sqs.MessageAction
import cats.instances.try_._
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model.{Message => SQSMessage}
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sqsworker.alpakka.{
  AlpakkaSQSWorker,
  AlpakkaSQSWorkerConfig
}
import uk.ac.wellcome.messaging.worker.models.{NonDeterministicFailure, Result}
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.bagreplicator.config.ReplicatorDestinationConfig
import uk.ac.wellcome.platform.archive.bagreplicator.models.ReplicationSummary
import uk.ac.wellcome.platform.archive.common.BagInformationPayload
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services._
import uk.ac.wellcome.platform.archive.common.storage.models.IngestStepWorker
import uk.ac.wellcome.storage.{LockDao, LockingService, ObjectLocation}
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.Future
import scala.util.Try

class BagReplicatorWorker[IngestsDestination, OutgoingDestination](
  alpakkaSQSWorkerConfig: AlpakkaSQSWorkerConfig,
  bagReplicator: BagReplicator,
  ingestUpdater: IngestUpdater[IngestsDestination],
  outgoingPublisher: OutgoingPublisher[OutgoingDestination],
  lockingService: LockingService[Result[ReplicationSummary],
                                 Try,
                                 LockDao[String, UUID]],
  replicatorDestinationConfig: ReplicatorDestinationConfig
)(implicit
  actorSystem: ActorSystem,
  mc: MonitoringClient,
  sc: AmazonSQSAsync)
    extends Runnable
    with Logging
    with IngestStepWorker {
  private val worker =
    new AlpakkaSQSWorker[
      BagInformationPayload,
      ReplicationSummary,
      MonitoringClient](alpakkaSQSWorkerConfig)(payload =>
      Future.fromTry(processMessage(payload))) {

      // TODO: This is hard-coded, read it from config!
      override val retryAction = (message: SQSMessage) =>
        (
          message,
          MessageAction.changeMessageVisibility(visibilityTimeout = 180))
    }

  val destinationBuilder = new DestinationBuilder(
    namespace = replicatorDestinationConfig.namespace,
    rootPath = replicatorDestinationConfig.rootPath
  )

  def processMessage(
    payload: BagInformationPayload,
  ): Try[Result[ReplicationSummary]] =
    for {
      _ <- ingestUpdater.start(payload.ingestId)

      destination = destinationBuilder.buildDestination(
        storageSpace = payload.storageSpace,
        externalIdentifier = payload.externalIdentifier,
        version = payload.version
      )

      result <- replicate(payload, destination)
    } yield result

  def replicate(payload: BagInformationPayload,
                destination: ObjectLocation): Try[Result[ReplicationSummary]] =
    lockingService
      .withLock(destination.toString) {
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
        } yield toResult(replicationSummary)
      }
      .map {
        case Right(result) => result
        case Left(failedLockingServiceOp) =>
          warn(s"Unable to lock successfully: $failedLockingServiceOp")
          NonDeterministicFailure(
            new Throwable(
              s"Unable to lock successfully: $failedLockingServiceOp"))
      }

  override def run(): Future[Any] = worker.start
}
