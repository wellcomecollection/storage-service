package uk.ac.wellcome.platform.archive.bagreplicator.services

import akka.actor.ActorSystem
import com.amazonaws.services.sqs.AmazonSQSAsync
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sqsworker.alpakka.{
  AlpakkaSQSWorker,
  AlpakkaSQSWorkerConfig
}
import uk.ac.wellcome.messaging.worker.models.Result
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.bagreplicator.models.ReplicationSummary
import uk.ac.wellcome.platform.archive.common.ObjectLocationPayload
import uk.ac.wellcome.platform.archive.common.ingests.models.BagRequest
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services._
import uk.ac.wellcome.platform.archive.common.storage.models.IngestStepWorker
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class BagReplicatorWorker(
  alpakkaSQSWorkerConfig: AlpakkaSQSWorkerConfig,
  bagReplicator: BagReplicator,
  ingestUpdater: IngestUpdater,
  outgoingPublisher: OutgoingPublisher
)(implicit
  actorSystem: ActorSystem,
  ec: ExecutionContext,
  mc: MonitoringClient,
  sc: AmazonSQSAsync)
    extends Runnable
    with Logging
    with IngestStepWorker {
  private val worker: AlpakkaSQSWorker[BagRequest, ReplicationSummary] =
    AlpakkaSQSWorker[BagRequest, ReplicationSummary](alpakkaSQSWorkerConfig) {
      bagRequest: BagRequest =>
        processMessage(bagRequest)
    }

  def processMessage(
    bagRequest: BagRequest): Future[Result[ReplicationSummary]] = {
    val payload = ObjectLocationPayload(
      ingestId = bagRequest.ingestId,
      storageSpace = bagRequest.bagLocation.storageSpace,
      objectLocation = bagRequest.bagLocation.objectLocation
    )

    processMessage(payload, bagRequest)
  }

  def processMessage(
    payload: ObjectLocationPayload,
    bagRequest: BagRequest
  ): Future[Result[ReplicationSummary]] =
    for {
      replicationSummary <- bagReplicator.replicate(
        bagRootLocation = payload.objectLocation,
        storageSpace = payload.storageSpace
      )
      _ <- ingestUpdater.send(payload.ingestId, replicationSummary)
      _ <- outgoingPublisher.sendIfSuccessful(
        replicationSummary,
        bagRequest.copy(
          bagLocation = replicationSummary.summary.destination
        )
      )
    } yield toResult(replicationSummary)

  override def run(): Future[Any] = worker.start
}
