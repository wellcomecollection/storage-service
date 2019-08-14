package uk.ac.wellcome.platform.storage.replica_aggregator.services

import akka.actor.ActorSystem
import com.amazonaws.services.sqs.AmazonSQSAsync
import io.circe.Decoder
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.common.EnrichedBagInformationPayload
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services.OutgoingPublisher
import uk.ac.wellcome.platform.archive.common.storage.models.{IngestFailed, IngestStepResult, IngestStepSucceeded, IngestStepWorker}
import uk.ac.wellcome.platform.storage.replica_aggregator.models._

import scala.util.{Success, Try}

class ReplicaAggregatorWorker[IngestDestination, OutgoingDestination](
    val config: AlpakkaSQSWorkerConfig,
    replicaAggregator: ReplicaAggregator,
    ingestUpdater: IngestUpdater[IngestDestination],
    outgoingPublisher: OutgoingPublisher[OutgoingDestination]
  )(
    implicit val mc: MonitoringClient,
    val as: ActorSystem,
    val sc: AmazonSQSAsync,
    val wd: Decoder[EnrichedBagInformationPayload]
  ) extends IngestStepWorker[
    EnrichedBagInformationPayload,
    ReplicationAggregationSummary
  ] {

  override def processMessage(
    payload: EnrichedBagInformationPayload
  ): Try[IngestStepResult[ReplicationAggregationSummary]] = for {

    aggregation <- replicaAggregator.aggregate(ReplicaResult(payload))

    // TODO: Need to distinguish result to determine outgoing message:
    // ReplicationAggregationIncomplete/ReplicationAggregationComplete
    // How does that map to IngestStepSucceeded & then sendIfSuccessful?

    ingestStep <- Success(aggregation match {
      case failed: ReplicationAggregationFailed => IngestFailed(failed, failed.e)
      case default => IngestStepSucceeded(default)
    })

    _ <- ingestUpdater.send(payload.ingestId, ingestStep)

    _ <- outgoingPublisher.sendIfSuccessful(ingestStep, payload)
  } yield ingestStep


}
