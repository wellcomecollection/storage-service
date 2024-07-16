package weco.storage_service.bag_versioner.services

import org.apache.pekko.actor.ActorSystem
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.messaging.sqsworker.pekko.PekkoSQSWorkerConfig
import weco.monitoring.Metrics
import weco.storage_service.ingests.models.{
  IngestEvent,
  IngestID,
  IngestVersionUpdate
}
import weco.storage_service.ingests.services.IngestUpdater
import weco.storage_service.operation.services._
import weco.storage_service.storage.models.{
  IngestStepResult,
  IngestStepSucceeded,
  IngestStepWorker
}
import weco.storage_service.{BagRootLocationPayload, VersionedBagRootPayload}
import weco.storage_service.bag_versioner.models.{
  BagVersionerSuccessSummary,
  BagVersionerSummary
}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Success, Try}

class BagVersionerWorker[IngestDestination, OutgoingDestination](
  val config: PekkoSQSWorkerConfig,
  bagVersioner: BagVersioner,
  ingestUpdater: IngestUpdater[IngestDestination],
  outgoingPublisher: OutgoingPublisher[OutgoingDestination]
)(
  implicit val mc: Metrics[Future],
  val as: ActorSystem,
  val sc: SqsAsyncClient,
  val wd: Decoder[BagRootLocationPayload]
) extends IngestStepWorker[BagRootLocationPayload, BagVersionerSummary] {

  // The bag versioner can fail if two bags with the same ID are being
  // processed at the same time, and one instance has the lock -- the other
  // instance will fail because it can't acquire a lock.
  //
  // If we're the other instance, we shouldn't retry immediately -- the
  // first instance will still have the lock.  Instead, spin for 30 seconds
  // and only then try again.
  override val visibilityTimeout: Duration = 30.seconds

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
          VersionedBagRootPayload(
            context = payload.context,
            bagRoot = payload.bagRoot,
            version = summary.version
          )
        )

      case _ => Success(())
    }
}
