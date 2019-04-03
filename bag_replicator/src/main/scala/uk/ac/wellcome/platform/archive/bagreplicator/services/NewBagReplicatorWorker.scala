package uk.ac.wellcome.platform.archive.bagreplicator.services

import akka.actor.ActorSystem
import com.amazonaws.services.sqs.AmazonSQSAsync
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sqsworker.alpakka.{AlpakkaSQSWorker, AlpakkaSQSWorkerConfig}
import uk.ac.wellcome.messaging.worker.models.{DeterministicFailure, Successful}
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.bagreplicator.models.{ReplicationCompleted, ReplicationSummary}
import uk.ac.wellcome.platform.archive.common.ingests.models.BagRequest
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services.{IngestCompleted, IngestFailed, IngestStepSuccess, OutgoingPublisher}
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class NewBagReplicatorWorker(
  alpakkaSQSWorkerConfig: AlpakkaSQSWorkerConfig,
  bagReplicator: BagReplicator,
  ingestUpdater: IngestUpdater,
  outgoingPublisher: OutgoingPublisher
)(implicit
  actorSystem: ActorSystem,
  ec: ExecutionContext,
  mc: MonitoringClient,
  sc: AmazonSQSAsync) extends Runnable with Logging {
  private val worker: AlpakkaSQSWorker[BagRequest, ReplicationSummary] =
    AlpakkaSQSWorker[BagRequest, ReplicationSummary](alpakkaSQSWorkerConfig) {
      bagRequest: BagRequest =>
        for {
          replicationResult <- bagReplicator.replicate(bagRequest.bagLocation)
          _ <- ingestUpdater.send(bagRequest.requestId, replicationResult)
          _ <- replicationResult.summary match {
            case ReplicationCompleted(_, dstLocation, _, _) =>
              outgoingPublisher.sendIfSuccessful(
                replicationResult,
                bagRequest.copy(bagLocation = dstLocation)
              )
            case _ => Future.successful(())
          }

          result = replicationResult match {
            case IngestStepSuccess(s) => Successful(Some(s))
            case IngestCompleted(s)   => Successful(Some(s))
            case IngestFailed(s, t)   => DeterministicFailure(t, Some(s))
          }
        } yield result
    }

  override def run(): Future[Any] = worker.start
}
