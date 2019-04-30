package uk.ac.wellcome.platform.archive.bagreplicator.services

import java.util.UUID

import akka.actor.ActorSystem
import com.amazonaws.services.sqs.AmazonSQSAsync
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sqsworker.alpakka.{AlpakkaSQSWorker, AlpakkaSQSWorkerConfig}
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.bagreplicator.models.ReplicationSummary
import uk.ac.wellcome.platform.archive.common.bagit.models.BagLocation
import uk.ac.wellcome.platform.archive.common.ingests.models.BagRequest
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services._
import uk.ac.wellcome.platform.archive.common.storage.models.{IngestStepResult, IngestStepWorker}
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class BagReplicatorWorker(
                           config: AlpakkaSQSWorkerConfig,
                           bagReplicator: BagReplicator,
                           ingestUpdater: IngestUpdater,
                           outgoingPublisher: OutgoingPublisher
)(implicit
  as: ActorSystem,
  ec: ExecutionContext,
  mc: MonitoringClient,
  sc: AmazonSQSAsync)
    extends Runnable
    with Logging
    with IngestStepWorker {

  type ReplicatorWorker =
    AlpakkaSQSWorker[BagRequest, ReplicationSummary]

  type IngestSummary =
    IngestStepResult[ReplicationSummary]

  private val replicate = (location: BagLocation) =>
    bagReplicator.replicate(location)

  private val updateIngest = (id: UUID, summary: IngestSummary) =>
    ingestUpdater.send(id, summary)

  private val publishOutgoing =
    (request: BagRequest, summary: IngestSummary) =>
      outgoingPublisher.sendIfSuccessful(summary,
          request.copy(
            bagLocation = summary.summary.destination
          )
        )

  val processMessage = (request: BagRequest)  =>
    for {
      summary <- replicate(request.bagLocation)
      _ <- updateIngest(request.requestId, summary)
      _ <- publishOutgoing(request, summary)
    } yield toResult(summary)

  private val worker: ReplicatorWorker =
    AlpakkaSQSWorker(config)(processMessage)

  override def run(): Future[_] = worker.start
}
