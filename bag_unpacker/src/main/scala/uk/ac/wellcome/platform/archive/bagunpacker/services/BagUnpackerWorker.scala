package uk.ac.wellcome.platform.archive.bagunpacker.services

import akka.actor.ActorSystem
import com.amazonaws.services.sqs.AmazonSQSAsync
import io.circe.Json
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sqsworker.alpakka.{
  AlpakkaSQSWorker,
  AlpakkaSQSWorkerConfig
}
import uk.ac.wellcome.messaging.worker.models.Result
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.bagunpacker.builders.BagLocationBuilder
import uk.ac.wellcome.platform.archive.bagunpacker.config.models.BagUnpackerWorkerConfig
import uk.ac.wellcome.platform.archive.bagunpacker.models.{
  BagUnpackerPayload,
  UnpackSummary
}
import uk.ac.wellcome.platform.archive.common.JsonPayloadWorker
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services._
import uk.ac.wellcome.platform.archive.common.storage.models.IngestStepWorker
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.Future
import scala.util.Try

case class BagUnpackerWorker[IngestDestination, OutgoingDestination](
  alpakkaSQSWorkerConfig: AlpakkaSQSWorkerConfig,
  bagUnpackerWorkerConfig: BagUnpackerWorkerConfig,
  ingestUpdater: IngestUpdater[IngestDestination],
  outgoingPublisher: OutgoingPublisher[OutgoingDestination],
  unpacker: Unpacker)(implicit
                      actorSystem: ActorSystem,
                      mc: MonitoringClient,
                      sc: AmazonSQSAsync)
    extends Runnable
    with IngestStepWorker
    with JsonPayloadWorker {
  private val worker =
    AlpakkaSQSWorker[Json, UnpackSummary](
      alpakkaSQSWorkerConfig) {
      json => Future.fromTry { processMessage(json) }
    }

  def processMessage(json: Json): Try[Result[UnpackSummary]] =
    for {
      payload <- asPayload[BagUnpackerPayload](json)

      unpackedBagLocation = BagLocationBuilder.build(
        ingestId = payload.ingestId,
        storageSpace = payload.storageSpace,
        unpackerWorkerConfig = bagUnpackerWorkerConfig
      )

      _ <- ingestUpdater.start(payload.ingestId)

      stepResult <- unpacker.unpack(
        requestId = payload.ingestId.toString,
        srcLocation = payload.sourceLocation,
        dstLocation = unpackedBagLocation
      )

<<<<<<< HEAD
      _ <- ingestUpdater.send(payload.ingestId, stepResult)

      outgoingPayload = addField(json)("unpackedBagLocation", unpackedBagLocation)
=======
      _ <- Future.fromTry {
        ingestUpdater.send(payload.ingestId, stepResult)
      }
      outgoingPayload = addField(json)(
        "unpackedBagLocation",
        unpackedBagLocation)
>>>>>>> Apply auto-formatting rules

      _ <- outgoingPublisher.sendIfSuccessful(stepResult, outgoingPayload)
    } yield toResult(stepResult)

  override def run(): Future[Any] = worker.start
}
