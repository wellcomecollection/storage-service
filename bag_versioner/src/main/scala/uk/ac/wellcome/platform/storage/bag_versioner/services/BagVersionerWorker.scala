package uk.ac.wellcome.platform.storage.bag_versioner.services

import akka.actor.ActorSystem
import com.amazonaws.services.sqs.AmazonSQSAsync
import io.circe.Decoder
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  IngestEvent,
  IngestID,
  IngestVersionUpdate
}
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services._
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestStepResult,
  IngestStepSucceeded,
  IngestStepWorker
}
import uk.ac.wellcome.platform.archive.common.{
  BagRootLocationPayload,
  EnrichedBagInformationPayload
}
import uk.ac.wellcome.platform.storage.bag_versioner.models.{
  BagVersionerSuccessSummary,
  BagVersionerSummary
}

import scala.util.{Success, Try}

class BagVersionerWorker[IngestDestination, OutgoingDestination](
  val config: AlpakkaSQSWorkerConfig,
  bagVersioner: BagVersioner,
  ingestUpdater: IngestUpdater[IngestDestination],
  outgoingPublisher: OutgoingPublisher[OutgoingDestination]
)(
  implicit val mc: MonitoringClient,
  val as: ActorSystem,
  val sc: AmazonSQSAsync,
  val wd: Decoder[BagRootLocationPayload]
) extends IngestStepWorker[BagRootLocationPayload, BagVersionerSummary] {

  override def processMessage(
    payload: BagRootLocationPayload
  ): Try[IngestStepResult[BagVersionerSummary]] =
    for {
      _ <- ingestUpdater.start(ingestId = payload.ingestId)

      stepResult <- bagVersioner.getSummary(
        ingestId = payload.ingestId,
        ingestDate = payload.ingestDate,
        ingestType = payload.ingestType,
        externalIdentifier = payload.externalIdentifier,
        storageSpace = payload.storageSpace
      )

      _ <- sendIngestUpdate(payload.ingestId, stepResult)
      _ <- sendSuccessful(payload)(stepResult)
    } yield stepResult

  private def sendIngestUpdate(
    ingestId: IngestID,
    stepResult: IngestStepResult[BagVersionerSummary]
  ): Try[Unit] =
    stepResult match {
      case IngestStepSucceeded(summary: BagVersionerSuccessSummary, _) =>
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

  private def sendSuccessful(
    payload: BagRootLocationPayload
  )(step: IngestStepResult[BagVersionerSummary]): Try[Unit] =
    step match {
      case IngestStepSucceeded(summary: BagVersionerSuccessSummary, _) =>
        outgoingPublisher.sendIfSuccessful(
          step,
          EnrichedBagInformationPayload(
            context = payload.context,
            bagRoot = payload.bagRoot,
            version = summary.version
          )
        )

      case _ => Success(())
    }
}
