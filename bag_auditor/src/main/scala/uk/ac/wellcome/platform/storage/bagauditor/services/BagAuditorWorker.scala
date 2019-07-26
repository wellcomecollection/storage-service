package uk.ac.wellcome.platform.storage.bagauditor.services

import akka.actor.ActorSystem
import com.amazonaws.services.sqs.AmazonSQSAsync
import io.circe.Decoder
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.common.ingests.models.{IngestEvent, IngestID, IngestVersionUpdate}
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services._
import uk.ac.wellcome.platform.archive.common.storage.models.{IngestStepResult, IngestStepSucceeded, IngestStepWorker}
import uk.ac.wellcome.platform.archive.common.{BagRootLocationPayload, EnrichedBagInformationPayload}
import uk.ac.wellcome.platform.storage.bagauditor.models.{AuditSuccessSummary, AuditSummary}

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

      stepResult <- bagAuditor.getAuditSummary(
        ingestId = payload.ingestId,
        ingestDate = payload.ingestDate,
        ingestType = payload.ingestType,
        externalIdentifier = payload.externalIdentifier,
        storageSpace = payload.storageSpace
      )

      _ <- sendIngestUpdate(payload.ingestId, stepResult)
      _ <- sendSuccessful(payload)(stepResult)
    } yield stepResult

  private def sendIngestUpdate(ingestId: IngestID, stepResult: IngestStepResult[AuditSummary]): Try[Unit] =
    stepResult match {
      case IngestStepSucceeded(summary: AuditSuccessSummary, _) =>
        val update = IngestVersionUpdate(
          id = ingestId,
          events = Seq(
            IngestEvent(
              s"${ingestUpdater.stepName.capitalize} succeeded - assigned bag version ${summary.version}"
            )
          ),
          version = summary.version
        )

        ingestUpdater.sendUpdate(update)

      case _ =>
        ingestUpdater.send(ingestId, stepResult)
    }

  private def sendSuccessful(payload: BagRootLocationPayload)(
    step: IngestStepResult[AuditSummary]): Try[Unit] =
    step match {
      case IngestStepSucceeded(summary: AuditSuccessSummary, _) =>
        outgoingPublisher.sendIfSuccessful(
          step,
          EnrichedBagInformationPayload(
            context = payload.context,
            bagRootLocation = payload.bagRootLocation,
            version = summary.version
          )
        )

      case _ => Success(())
    }
}
