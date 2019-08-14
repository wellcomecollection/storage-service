package uk.ac.wellcome.platform.storage.replica_aggregator.services

import java.time.Instant

import akka.actor.ActorSystem
import com.amazonaws.services.sqs.AmazonSQSAsync
import io.circe.Decoder
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.common.EnrichedBagInformationPayload
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services.OutgoingPublisher
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestStepResult,
  IngestStepSucceeded,
  IngestStepWorker
}
import uk.ac.wellcome.platform.storage.replica_aggregator.models._

import scala.util.Try

class ReplicaAggregatorWorker[IngestDestination, OutgoingDestination](
  val config: AlpakkaSQSWorkerConfig,
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
  ): Try[IngestStepResult[ReplicationAggregationSummary]] = {
    val replicaResult = ReplicaResult(payload)

    val replicationSet = ReplicationSet(
      path = ReplicaPath(payload.bagRoot.path),
      results = Set(replicaResult)
    )

    val summary = ReplicationAggregationComplete(
      replicationSet = replicationSet,
      startTime = Instant.now,
      endTime = Instant.now
    )

    val ingestStep = IngestStepSucceeded(summary)

    for {
      _ <- ingestUpdater.send(payload.ingestId, ingestStep)
      _ <- outgoingPublisher.sendIfSuccessful(ingestStep, payload)
    } yield ingestStep
  }

}
