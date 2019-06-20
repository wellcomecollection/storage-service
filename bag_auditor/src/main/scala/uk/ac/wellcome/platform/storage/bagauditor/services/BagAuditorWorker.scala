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
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services._
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestStepResult,
  IngestStepSucceeded,
  IngestStepWorker
}
import uk.ac.wellcome.platform.archive.common.{
  EnrichedBagInformationPayload,
  UnpackedBagLocationPayload
}
import uk.ac.wellcome.platform.storage.bagauditor.models.{
  AuditSuccessSummary,
  AuditSummary
}
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.Future
import scala.util.{Success, Try}

class BagAuditorWorker[IngestDestination, OutgoingDestination](
  alpakkaSQSWorkerConfig: AlpakkaSQSWorkerConfig,
  bagAuditor: BagAuditor,
  ingestUpdater: IngestUpdater[IngestDestination],
  outgoingPublisher: OutgoingPublisher[OutgoingDestination]
)(implicit
  actorSystem: ActorSystem,
  mc: MonitoringClient,
  sc: AmazonSQSAsync)
    extends Runnable
    with Logging
    with IngestStepWorker {
  private val worker =
    AlpakkaSQSWorker[UnpackedBagLocationPayload, AuditSummary](alpakkaSQSWorkerConfig) {
      payload: UnpackedBagLocationPayload =>
        Future.fromTry { processMessage(payload) }
    }

  def processMessage(payload: UnpackedBagLocationPayload): Try[Result[AuditSummary]] =
    for {
      _ <- ingestUpdater.start(ingestId = payload.ingestId)

      auditStep <- bagAuditor.getAuditSummary(
        ingestId = payload.ingestId,
        ingestDate = payload.ingestDate,
        unpackLocation = payload.unpackedBagLocation,
        storageSpace = payload.storageSpace
      )

      _ <- sendIngestInformation(payload)(auditStep)
      _ <- ingestUpdater.send(payload.ingestId, auditStep)
      _ <- sendSuccessful(payload)(auditStep)
    } yield toResult(auditStep)

  private def sendIngestInformation(payload: UnpackedBagLocationPayload)(
    step: IngestStepResult[AuditSummary]): Try[Unit] =
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
      case _ => Success(())
    }

  private def sendSuccessful(payload: UnpackedBagLocationPayload)(
    step: IngestStepResult[AuditSummary]): Try[Unit] =
    step match {
      case IngestStepSucceeded(summary: AuditSuccessSummary) =>
        outgoingPublisher.sendIfSuccessful(
          step,
          EnrichedBagInformationPayload(
            context = payload.context,
            bagRootLocation = summary.audit.root,
            externalIdentifier = summary.audit.externalIdentifier,
            version = summary.audit.version
          )
        )
      case _ => Success(())
    }

  override def run(): Future[Any] = worker.start
}
