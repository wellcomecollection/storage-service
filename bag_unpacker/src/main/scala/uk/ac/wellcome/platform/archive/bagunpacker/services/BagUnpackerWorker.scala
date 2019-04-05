package uk.ac.wellcome.platform.archive.bagunpacker.services

import akka.actor.ActorSystem
import com.amazonaws.services.sqs.AmazonSQSAsync
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sqsworker.alpakka.{
  AlpakkaSQSWorker,
  AlpakkaSQSWorkerConfig
}
import uk.ac.wellcome.messaging.worker.monitoring.MonitoringClient
import uk.ac.wellcome.platform.archive.bagunpacker.builders.BagLocationBuilder
import uk.ac.wellcome.platform.archive.bagunpacker.config.models.BagUnpackerWorkerConfig
import uk.ac.wellcome.platform.archive.bagunpacker.models.UnpackSummary
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  BagRequest,
  UnpackBagRequest
}
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services._
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

case class BagUnpackerWorker(alpakkaSQSWorkerConfig: AlpakkaSQSWorkerConfig,
                             bagUnpackerWorkerConfig: BagUnpackerWorkerConfig,
                             ingestUpdater: IngestUpdater,
                             outgoingPublisher: OutgoingPublisher,
                             unpacker: Unpacker)(implicit ac: ActorSystem,
                                                 ec: ExecutionContext,
                                                 mc: MonitoringClient,
                                                 sc: AmazonSQSAsync)
    extends Runnable
    with Logging
    with IngestStepWorker {
  private val worker: AlpakkaSQSWorker[UnpackBagRequest, UnpackSummary] =
    AlpakkaSQSWorker[UnpackBagRequest, UnpackSummary](alpakkaSQSWorkerConfig) {
      unpackBagRequest: UnpackBagRequest =>
        val location =
          BagLocationBuilder.build(unpackBagRequest, bagUnpackerWorkerConfig)
        for {
          unpackSummary <- unpacker.unpack(
            unpackBagRequest.requestId.toString,
            unpackBagRequest.sourceLocation,
            location.objectLocation)
          _ <- ingestUpdater.send(unpackBagRequest.requestId, unpackSummary)
          _ <- outgoingPublisher.sendIfSuccessful(
            unpackSummary,
            BagRequest(unpackBagRequest.requestId, location))
        } yield toResult(unpackSummary)
    }
  override def run(): Future[Any] = worker.start
}
