package uk.ac.wellcome.platform.storage.bagauditor.services

import akka.actor.ActorSystem
import com.amazonaws.services.sqs.AmazonSQSAsync
import io.circe.Decoder
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services._
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestStepResult,
  IngestStepSucceeded,
  IngestStepWorker
}
import uk.ac.wellcome.platform.archive.common.{
  BagRootLocationPayload,
  VersionedBagRootLocationPayload
}
import uk.ac.wellcome.platform.storage.bagauditor.models.{
  AuditSuccessSummary,
  AuditSummary
}

import scala.util.{Success, Try}

class BagAuditorWorker[IngestDestination, OutgoingDestination](
  val config: AlpakkaSQSWorkerConfig,
  bagAuditor: BagAuditor,
  ingestUpdater: IngestUpdater[IngestDestination],
  outgoingPublisher: OutgoingPublisher[OutgoingDestination]
)(implicit val mc: MonitoringClient,
  val as: ActorSystem,
  val sc: AmazonSQSAsync,
  val wd: Decoder[BagRootLocationPayload])
    extends IngestStepWorker[BagRootLocationPayload, AuditSummary] {

  override def processMessage(
    payload: BagRootLocationPayload): Try[IngestStepResult[AuditSummary]] =
    for {
      _ <- ingestUpdater.start(ingestId = payload.ingestId)

      auditStep <- bagAuditor.getAuditSummary(
        ingestId = payload.ingestId,
        ingestDate = payload.ingestDate,
        ingestType = payload.ingestType,
        externalIdentifier = payload.externalIdentifier,
        storageSpace = payload.storageSpace
      )

      _ <- sendIngestInformation(payload)(auditStep)
      _ <- ingestUpdater.send(payload.ingestId, auditStep)
      _ <- sendSuccessful(payload)(auditStep)
    } yield auditStep

  private def sendIngestInformation(payload: BagRootLocationPayload)(
    step: IngestStepResult[AuditSummary]): Try[Unit] =
    step match {
      case IngestStepSucceeded(summary: AuditSuccessSummary) =>
        ingestUpdater.sendEvent(
          ingestId = payload.ingestId,
          messages = Seq(
            s"Assigned bag version ${summary.version}"
          )
        )
      case _ => Success(())
    }

  private def sendSuccessful(payload: BagRootLocationPayload)(
    step: IngestStepResult[AuditSummary]): Try[Unit] =
    step match {
      case IngestStepSucceeded(summary: AuditSuccessSummary) =>
        outgoingPublisher.sendIfSuccessful(
          step,
          VersionedBagRootLocationPayload(
            context = payload.context,
            bagRootLocation = payload.bagRootLocation,
            version = summary.version
          )
        )

      case _ => Success(())
    }
}
