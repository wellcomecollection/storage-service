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
  IngestFailed,
  IngestStepResult,
  IngestStepSucceeded,
  IngestStepWorker
}
import uk.ac.wellcome.platform.storage.replica_aggregator.models._

import scala.util.{Failure, Success, Try}

class ReplicaAggregatorWorker[IngestDestination, OutgoingDestination](
  val config: AlpakkaSQSWorkerConfig,
  replicaAggregator: ReplicaAggregator,
  replicaCounter: ReplicaCounter,
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
    val replicaPath = ReplicaPath(payload.bagRootLocation.path)

    val startTime = Instant.now()

    val trySummary =
      for {
        aggregatorRecord <- replicaAggregator.aggregate(ReplicaResult(payload))

        summary = replicaCounter.countReplicas(aggregatorRecord) match {
          case Right(knownReplicas) =>
            ReplicationAggregationComplete(
              replicaPath = replicaPath,
              knownReplicas = knownReplicas,
              startTime = startTime,
              endTime = Instant.now()
            )

          case Left(counterError) =>
            ReplicationAggregationIncomplete(
              replicaPath = replicaPath,
              aggregatorRecord = aggregatorRecord,
              counterError = counterError,
              startTime = startTime,
              endTime = Instant.now()
            )
        }
      } yield summary

    val ingestStep = trySummary match {
      case Success(replicaSummary) =>
        IngestStepSucceeded(replicaSummary)

      case Failure(err) =>
        IngestFailed(
          summary = ReplicationAggregationFailed(
            e = err,
            replicaPath = replicaPath,
            startTime = startTime,
            endTime = Instant.now()
          ),
          e = err
        )
    }

    for {
      _ <- ingestUpdater.send(payload.ingestId, ingestStep)
      _ <- sendOutgoing(ingestStep, payload)
    } yield ingestStep
  }

  private def sendOutgoing(
    ingestStep: IngestStepResult[ReplicationAggregationSummary],
    payload: EnrichedBagInformationPayload
  ): Try[Unit] =
    ingestStep match {
      case IngestStepSucceeded(_: ReplicationAggregationComplete, _) =>
        outgoingPublisher.sendIfSuccessful(ingestStep, payload)

      case _ =>
        Success(())
    }
}
