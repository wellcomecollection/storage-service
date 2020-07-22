package uk.ac.wellcome.platform.storage.replica_aggregator.services

import java.time.Instant

import akka.actor.ActorSystem
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.messaging.worker.monitoring.metrics.MetricsMonitoringClient
import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services.OutgoingPublisher
import uk.ac.wellcome.platform.archive.common.storage.models._
import uk.ac.wellcome.platform.archive.common.{
  KnownReplicasPayload,
  PipelineContext,
  ReplicaResultPayload
}
import uk.ac.wellcome.platform.storage.replica_aggregator.models._
import uk.ac.wellcome.storage.{RetryableError, UpdateError, UpdateWriteError}

import scala.util.{Success, Try}

class ReplicaAggregatorWorker[IngestDestination, OutgoingDestination](
  val config: AlpakkaSQSWorkerConfig,
  replicaAggregator: ReplicaAggregator,
  replicaCounter: ReplicaCounter,
  ingestUpdater: IngestUpdater[IngestDestination],
  outgoingPublisher: OutgoingPublisher[OutgoingDestination],
  val metricsNamespace: String
)(
  implicit val mc: MetricsMonitoringClient,
  val as: ActorSystem,
  val sc: SqsAsyncClient,
  val wd: Decoder[ReplicaResultPayload]
) extends IngestStepWorker[
      ReplicaResultPayload,
      ReplicationAggregationSummary
    ] {

  private sealed trait WorkerError
  private case class AggregationFailure(e: UpdateError) extends WorkerError

  private case class InsufficientReplicas(
    replicaCounterError: ReplicaCounterError,
    aggregatorRecord: AggregatorInternalRecord
  ) extends WorkerError

  private def getKnownReplicas(
    storageLocation: StorageLocation
  ): Either[WorkerError, KnownReplicas] =
    for {

      aggregatorRecord <- replicaAggregator
        .aggregate(storageLocation)
        .left
        .map(AggregationFailure)

      sufficientReplicas <- replicaCounter
        .countReplicas(aggregatorRecord)
        .left
        .map(InsufficientReplicas(_, aggregatorRecord))

    } yield sufficientReplicas

  override def processMessage(
    payload: ReplicaResultPayload
  ): Try[IngestStepResult[ReplicationAggregationSummary]] = {
    val replicaPath = ReplicaPath(payload.bagRoot.path)
    val startTime = Instant.now()

    val ingestStep = getKnownReplicas(payload.dstLocation) match {
      // If we get a retryable error when trying to store the replica
      // (for example, a DynamoDB ConditionalUpdate error), we want to retry
      // it rather than failing the entire ingest.
      //
      // The first case covers a ConditionalUpdate failure from DynamoDB;
      // the second a retryable error from inside VersionedStore.upsert().
      //
      case Left(AggregationFailure(UpdateWriteError(err: RetryableError))) =>
        IngestShouldRetry(
          summary = ReplicationAggregationFailed(
            ingestId = payload.ingestId,
            e = err.e,
            replicaPath = replicaPath,
            startTime = startTime,
            endTime = Instant.now()
          ),
          e = err.e
        )

      case Left(AggregationFailure(err: RetryableError)) =>
        IngestShouldRetry(
          summary = ReplicationAggregationFailed(
            ingestId = payload.ingestId,
            e = err.e,
            replicaPath = replicaPath,
            startTime = startTime,
            endTime = Instant.now()
          ),
          e = err.e
        )

      case Left(AggregationFailure(err)) =>
        IngestFailed(
          summary = ReplicationAggregationFailed(
            ingestId = payload.ingestId,
            e = err.e,
            replicaPath = replicaPath,
            startTime = startTime,
            endTime = Instant.now()
          ),
          e = err.e
        )

      case Left(InsufficientReplicas(err, aggregatorRecord)) =>
        IngestStepSucceeded(
          ReplicationAggregationIncomplete(
            ingestId = payload.ingestId,
            replicaPath = replicaPath,
            aggregatorRecord = aggregatorRecord,
            counterError = err,
            startTime = startTime,
            endTime = Instant.now()
          ),
          maybeUserFacingMessage = Some(
            s"${aggregatorRecord.count} of ${replicaCounter.expectedReplicaCount} replicas complete"
          )
        )

      case Right(knownReplicas: KnownReplicas) =>
        IngestStepSucceeded(
          ReplicationAggregationComplete(
            ingestId = payload.ingestId,
            replicaPath = replicaPath,
            knownReplicas = knownReplicas,
            startTime = startTime,
            endTime = Instant.now()
          ),
          maybeUserFacingMessage = Some("all replicas complete")
        )
    }

    for {
      _ <- ingestUpdater.send(payload.ingestId, ingestStep)
      _ <- sendOutgoing(
        ingestStep = ingestStep,
        context = payload.context,
        version = payload.version
      )
    } yield ingestStep
  }

  private def sendOutgoing(
    ingestStep: IngestStepResult[ReplicationAggregationSummary],
    context: PipelineContext,
    version: BagVersion
  ): Try[Unit] =
    ingestStep match {

      // We only want to notify the rest of the pipeline
      // when an aggregation completes with sufficient replicas.
      //
      // We do not want to notify the pipeline when:
      // - IngestStepSucceeded<ReplicationAggregationIncomplete>

      // TODO: Think about having an IngestStep status that represents
      // success but no outgoing state.

      case IngestStepSucceeded(complete: ReplicationAggregationComplete, _) =>
        val payload = KnownReplicasPayload(
          context = context,
          version = version,
          knownReplicas = complete.knownReplicas
        )
        outgoingPublisher.send(payload)

      case _ =>
        Success(())
    }
}
