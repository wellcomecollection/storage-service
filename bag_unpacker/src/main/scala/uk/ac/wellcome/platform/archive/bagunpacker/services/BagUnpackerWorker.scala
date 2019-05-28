package uk.ac.wellcome.platform.archive.bagunpacker.services

import akka.actor.ActorSystem
import com.amazonaws.services.sqs.AmazonSQSAsync
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sqsworker.alpakka.{
  AlpakkaSQSWorker,
  AlpakkaSQSWorkerConfig
}
import uk.ac.wellcome.messaging.worker.models.Result
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.bagunpacker.builders.BagLocationBuilder
import uk.ac.wellcome.platform.archive.bagunpacker.config.models.BagUnpackerWorkerConfig
import uk.ac.wellcome.platform.archive.bagunpacker.models.UnpackSummary
import uk.ac.wellcome.platform.archive.common.{
  IngestRequestPayload,
  UnpackedBagPayload
}
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services._
import uk.ac.wellcome.platform.archive.common.storage.models.IngestStepWorker
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

case class BagUnpackerWorker[IngestDestination, OutgoingDestination](
  alpakkaSQSWorkerConfig: AlpakkaSQSWorkerConfig,
  bagUnpackerWorkerConfig: BagUnpackerWorkerConfig,
  ingestUpdater: IngestUpdater[IngestDestination],
  outgoingPublisher: OutgoingPublisher[OutgoingDestination],
  unpacker: Unpacker)(
  implicit
  actorSystem: ActorSystem,
  ec: ExecutionContext,
  mc: MonitoringClient,
  sc: AmazonSQSAsync)
    extends Runnable
    with IngestStepWorker {
  private val worker =
    AlpakkaSQSWorker[IngestRequestPayload, UnpackSummary](
      alpakkaSQSWorkerConfig) {
      processMessage
    }

  def processMessage(
    payload: IngestRequestPayload): Future[Result[UnpackSummary]] = {
    val unpackedBagLocation = BagLocationBuilder.build(
      ingestId = payload.ingestId,
      storageSpace = payload.storageSpace,
      unpackerWorkerConfig = bagUnpackerWorkerConfig
    )
    for {
      _ <- Future.fromTry {
        ingestUpdater.start(payload.ingestId)
      }

      stepResult <- unpacker.unpack(
        requestId = payload.ingestId.toString,
        srcLocation = payload.sourceLocation,
        dstLocation = unpackedBagLocation
      )

      _ <- Future.fromTry {
        ingestUpdater.send(payload.ingestId, stepResult)
      }
      outgoingPayload = UnpackedBagPayload(
        ingestRequestPayload = payload,
        unpackedBagLocation = unpackedBagLocation
      )
      _ <- Future.fromTry {
        outgoingPublisher.sendIfSuccessful(stepResult, outgoingPayload)
      }
    } yield toResult(stepResult)
  }

  override def run(): Future[Any] = worker.start
}
