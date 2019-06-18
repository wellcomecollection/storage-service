package uk.ac.wellcome.platform.storage.bag_root_finder.services

import akka.actor.ActorSystem
import com.amazonaws.services.sqs.AmazonSQSAsync
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sqsworker.alpakka.{AlpakkaSQSWorker, AlpakkaSQSWorkerConfig}
import uk.ac.wellcome.messaging.worker.models.Result
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services._
import uk.ac.wellcome.platform.archive.common.storage.models.{IngestStepResult, IngestStepSucceeded, IngestStepWorker}
import uk.ac.wellcome.platform.archive.common.{BagInformationPayload, BagRootPayload, UnpackedBagPayload}
import uk.ac.wellcome.platform.storage.bag_root_finder.models.{RootFinderSuccessSummary, RootFinderSummary}
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.Future
import scala.util.{Success, Try}

class BagRootFinderWorker[IngestDestination, OutgoingDestination](
  alpakkaSQSWorkerConfig: AlpakkaSQSWorkerConfig,
  bagRootFinder: BagRootFinder,
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
    AlpakkaSQSWorker[UnpackedBagPayload, RootFinderSummary](alpakkaSQSWorkerConfig) {
      payload: UnpackedBagPayload =>
        Future.fromTry { processMessage(payload) }
    }

  def processMessage(payload: UnpackedBagPayload): Try[Result[RootFinderSummary]] =
    for {
      _ <- ingestUpdater.start(ingestId = payload.ingestId)

      summary <- bagRootFinder.getSummary(
        ingestId = payload.ingestId,
        unpackLocation = payload.unpackedBagLocation,
        storageSpace = payload.storageSpace
      )

      _ <- sendIngestInformation(payload)(summary)
      _ <- ingestUpdater.send(payload.ingestId, summary)
      _ <- sendSuccessful(payload)(summary)
    } yield toResult(summary)

  private def sendIngestInformation(payload: UnpackedBagPayload)(
    step: IngestStepResult[RootFinderSummary]): Try[Unit] =
    step match {
      case IngestStepSucceeded(summary: RootFinderSuccessSummary) =>
        ingestUpdater.sendEvent(
          ingestId = payload.ingestId,
          messages = Seq(
            s"Detected bag root as ${summary.bagRootLocation}",
          )
        )
      case _ => Success(())
    }

  private def sendSuccessful(payload: UnpackedBagPayload)(
    step: IngestStepResult[RootFinderSummary]): Try[Unit] =
    step match {
      case IngestStepSucceeded(summary: RootFinderSuccessSummary) =>
        val outgoingPayload: BagRootPayload = BagInformationPayload(
          ingestId = payload.ingestId,
          storageSpace = payload.storageSpace,
          bagRootLocation = summary.bagRootLocation,
        )
        outgoingPublisher.sendIfSuccessful(step, outgoingPayload)
      case _ => Success(())
    }

  override def run(): Future[Any] = worker.start
}
