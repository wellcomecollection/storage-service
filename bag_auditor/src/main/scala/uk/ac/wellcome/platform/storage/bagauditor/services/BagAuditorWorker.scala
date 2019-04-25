package uk.ac.wellcome.platform.storage.bagauditor.services

import java.util.UUID

import akka.actor.ActorSystem
import com.amazonaws.services.sqs.AmazonSQSAsync
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sqsworker.alpakka.{AlpakkaSQSWorker, AlpakkaSQSWorkerConfig}
import uk.ac.wellcome.messaging.worker.models.Result
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.common.bagit.models.BagLocation
import uk.ac.wellcome.platform.archive.common.ingests.models.BagRequest
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services._
import uk.ac.wellcome.platform.archive.common.storage.models.IngestStepWorker
import uk.ac.wellcome.platform.storage.bagauditor.models.AuditSummary
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

case class BetterBagRequest(
  requestId: UUID,
  bagLocation: BagLocation,
  bagRoot: ObjectLocation
)

class BagAuditorWorker(
  alpakkaSQSWorkerConfig: AlpakkaSQSWorkerConfig,
  bagAuditor: BagAuditor,
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
  private val worker: AlpakkaSQSWorker[BagRequest, AuditSummary] =
    AlpakkaSQSWorker[BagRequest, AuditSummary](alpakkaSQSWorkerConfig) {
      bagRequest: BagRequest =>
        processMessage(bagRequest)
    }

  def processMessage(bagRequest: BagRequest): Future[Result[AuditSummary]] =
    for {
      auditSummary <- Future.fromTry(bagAuditor.locateBagRoot(bagRequest.bagLocation))
      _ <- ingestUpdater.send(bagRequest.requestId, auditSummary)
      _ <- outgoingPublisher.sendIfSuccessful(
        auditSummary,
        BetterBagRequest(
          requestId = bagRequest.requestId,
          bagLocation = bagRequest.bagLocation,
          bagRoot = auditSummary.summary.root
        )
      )
    } yield toResult(auditSummary)

  override def run(): Future[Any] = worker.start
}
