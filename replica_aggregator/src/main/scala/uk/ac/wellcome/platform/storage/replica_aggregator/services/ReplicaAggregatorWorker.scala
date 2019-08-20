package uk.ac.wellcome.platform.storage.replica_aggregator.services

import java.time.Instant

import akka.actor.ActorSystem
import com.amazonaws.services.sqs.AmazonSQSAsync
import io.circe.Decoder
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services.OutgoingPublisher
import uk.ac.wellcome.platform.archive.common.storage.models._
import uk.ac.wellcome.platform.archive.common.{
  EnrichedBagInformationPayload,
  KnownReplicasPayload,
  PipelineContext
}
import uk.ac.wellcome.platform.storage.replica_aggregator.models._

import scala.util.{Success, Try}

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

  private sealed trait WorkerError
  private case class AggregationFailure(e: Throwable) extends WorkerError
  private case class InsufficientReplicas(
                                   replicaCounterError: ReplicaCounterError,
                                   aggregatorRecord: AggregatorInternalRecord
                                 ) extends WorkerError

  private def getKnownReplicas(payload: EnrichedBagInformationPayload)
    : Either[WorkerError, KnownReplicas] =
    for {
      aggregatorRecord <- replicaAggregator
        .aggregate(ReplicaResult(payload))
        .toEither
        .left
        .map(AggregationFailure)

      sufficientReplicas <- replicaCounter
        .countReplicas(aggregatorRecord)
        .left
        .map(InsufficientReplicas(_, aggregatorRecord))

    } yield sufficientReplicas

  override def processMessage(
    payload: EnrichedBagInformationPayload
  ): Try[IngestStepResult[ReplicationAggregationSummary]] = {
    val replicaPath = ReplicaPath(payload.bagRootLocation.path)
    val startTime = Instant.now()

    val ingestStep = getKnownReplicas(payload) match {
      case Left(AggregationFailure(err)) =>
        IngestFailed(
          summary = ReplicationAggregationFailed(
            e = err,
            replicaPath = replicaPath,
            startTime = startTime,
            endTime = Instant.now()
          ),
          e = err
        )

      case Left(InsufficientReplicas(err, aggregatorRecord)) =>
        IngestStepSucceeded(
          ReplicationAggregationIncomplete(
            replicaPath = replicaPath,
            aggregatorRecord = aggregatorRecord,
            counterError = err,
            startTime = startTime,
            endTime = Instant.now()
          ))

      case Right(knownReplicas: KnownReplicas) =>
        IngestStepSucceeded(
          ReplicationAggregationComplete(
            replicaPath = replicaPath,
            knownReplicas = knownReplicas,
            startTime = startTime,
            endTime = Instant.now()
          )
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
