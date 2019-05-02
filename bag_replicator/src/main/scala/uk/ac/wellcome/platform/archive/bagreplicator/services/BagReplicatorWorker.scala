package uk.ac.wellcome.platform.archive.bagreplicator.services

import java.util.UUID

import akka.actor.ActorSystem
import com.amazonaws.services.sqs.AmazonSQSAsync
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sqsworker.alpakka.{AlpakkaSQSWorker, AlpakkaSQSWorkerConfig}
import uk.ac.wellcome.messaging.worker.models.Result
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.bagreplicator.config.ReplicatorDestinationConfig
import uk.ac.wellcome.platform.archive.bagreplicator.models.ReplicationSummary
import uk.ac.wellcome.platform.archive.common.BagInformationPayload
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services._
import uk.ac.wellcome.platform.archive.common.storage.models.{IngestStepResult, IngestStepWorker}
import uk.ac.wellcome.storage.locking.DynamoLockDao
import uk.ac.wellcome.storage.{LockDao, LockingService, ObjectLocation}
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}
import scala.language.higherKinds

class BetterDynamoLockingService[Out, OutMonad[_]](implicit val lockDao: DynamoLockDao)
  extends LockingService[Out, OutMonad, LockDao[String, UUID]] {
  override protected def createContextId(): lockDao.ContextId =
    UUID.randomUUID()
}

class BagReplicatorWorker(
  alpakkaSQSWorkerConfig: AlpakkaSQSWorkerConfig,
  bagReplicator: BagReplicator,
  ingestUpdater: IngestUpdater,
  outgoingPublisher: OutgoingPublisher,
  lockingService: LockingService[IngestStepResult[ReplicationSummary], Future, LockDao[String, UUID]],
  replicatorDestinationConfig: ReplicatorDestinationConfig
)(implicit
  actorSystem: ActorSystem,
  ec: ExecutionContext,
  mc: MonitoringClient,
  sc: AmazonSQSAsync)
    extends Runnable
    with Logging
    with IngestStepWorker {
  private val worker =
    AlpakkaSQSWorker[BagInformationPayload, ReplicationSummary](
      alpakkaSQSWorkerConfig) {
      processMessage
    }

  val destinationBuilder = new DestinationBuilder(
    namespace = replicatorDestinationConfig.namespace,
    rootPath = replicatorDestinationConfig.rootPath
  )

  def processMessage(
    payload: BagInformationPayload,
  ): Future[Result[ReplicationSummary]] =
    for {
      _ <- ingestUpdater.start(payload.ingestId)

      destination = destinationBuilder.buildDestination(
        storageSpace = payload.storageSpace,
        externalIdentifier = payload.externalIdentifier,
        version = payload.version
      )

      replicationSummary <- replicate(payload, destination)
      _ <- ingestUpdater.send(payload.ingestId, replicationSummary)
      _ <- outgoingPublisher.sendIfSuccessful(
        replicationSummary,
        payload.copy(
          bagRootLocation = replicationSummary.summary.destination
        )
      )
    } yield toResult(replicationSummary)

  def replicate(payload: BagInformationPayload, destination: ObjectLocation): Future[IngestStepResult[ReplicationSummary]] =
    bagReplicator.replicate(
      bagRootLocation = payload.bagRootLocation,
      destination = destination,
      storageSpace = payload.storageSpace
    )

  override def run(): Future[Any] = worker.start
}
