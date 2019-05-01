package uk.ac.wellcome.platform.storage.bagauditor.services

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
import uk.ac.wellcome.platform.archive.common.{
  BagInformationPayload,
  ObjectLocationPayload
}
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services._
import uk.ac.wellcome.platform.archive.common.storage.models.IngestStepWorker
import uk.ac.wellcome.platform.storage.bagauditor.models.AuditSummary
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

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
  private val worker: AlpakkaSQSWorker[ObjectLocationPayload, AuditSummary] =
    AlpakkaSQSWorker[ObjectLocationPayload, AuditSummary](
      alpakkaSQSWorkerConfig) {
      processMessage
    }

  def processMessage(
    payload: ObjectLocationPayload): Future[Result[AuditSummary]] =
    for {
      auditSummary <- bagAuditor.getAuditSummary(
        unpackLocation = payload.objectLocation,
        storageSpace = payload.storageSpace
      )
      _ <- ingestUpdater.send(payload.ingestId, auditSummary)
      _ <- outgoingPublisher.sendIfSuccessful(
        auditSummary,
        BagInformationPayload(
          ingestId = payload.ingestId,
          storageSpace = payload.storageSpace,
          bagRootLocation = auditSummary.summary.auditInformation.bagRootLocation,
          externalIdentifier =
            auditSummary.summary.auditInformation.externalIdentifier,
          version = auditSummary.summary.auditInformation.version
        )
      )
    } yield toResult(auditSummary)

  override def run(): Future[Any] = worker.start
}
