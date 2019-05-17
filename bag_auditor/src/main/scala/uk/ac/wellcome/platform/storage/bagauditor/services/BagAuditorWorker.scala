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
  UnpackedBagPayload
}
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services._
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestStepResult,
  IngestStepSucceeded,
  IngestStepWorker
}
import uk.ac.wellcome.platform.storage.bagauditor.models.{
  AuditSuccessSummary,
  AuditSummary
}
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
  private val worker =
    AlpakkaSQSWorker[UnpackedBagPayload, AuditSummary](alpakkaSQSWorkerConfig) {
      processMessage
    }

  def processMessage(
    payload: UnpackedBagPayload): Future[Result[AuditSummary]] =
    for {
      _ <- ingestUpdater.start(ingestId = payload.ingestId)

      auditStep <- Future.fromTry {
        bagAuditor.getAuditSummary(
          unpackLocation = payload.unpackedBagLocation,
          storageSpace = payload.storageSpace
        )
      }

      _ <- sendIngestInformation(payload)(auditStep)
      _ <- ingestUpdater.send(payload.ingestId, auditStep)
      _ <- sendSuccessful(payload)(auditStep)
    } yield toResult(auditStep)

  private def sendIngestInformation(payload: UnpackedBagPayload)(
    step: IngestStepResult[AuditSummary]): Future[Unit] =
    step match {
      case IngestStepSucceeded(summary: AuditSuccessSummary) =>
        ingestUpdater.sendEvent(
          ingestId = payload.ingestId,
          messages = Seq(
            s"Detected bag root as ${summary.audit.root}",
            s"Detected bag identifier as ${summary.audit.externalIdentifier}",
            s"Assigned bag version ${summary.audit.version}"
          )
        )
      case _ => Future.successful(())
    }

  private def sendSuccessful(payload: UnpackedBagPayload)(
    step: IngestStepResult[AuditSummary]): Future[Unit] =
    step match {
      case IngestStepSucceeded(summary: AuditSuccessSummary) =>
        outgoingPublisher.sendIfSuccessful(
          step,
          BagInformationPayload(
            ingestId = payload.ingestId,
            storageSpace = payload.storageSpace,
            bagRootLocation = summary.audit.root,
            externalIdentifier = summary.audit.externalIdentifier,
            version = summary.audit.version
          )
        )
      case _ => Future.successful(())
    }

  override def run(): Future[Any] = worker.start
}
