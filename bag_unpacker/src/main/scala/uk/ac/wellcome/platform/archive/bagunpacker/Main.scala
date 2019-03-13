package uk.ac.wellcome.platform.archive.bagunpacker

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.Config
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.typesafe.NotificationStreamBuilder
import uk.ac.wellcome.platform.archive.bagunpacker.config.UnpackerConfig
import uk.ac.wellcome.platform.archive.bagunpacker.services.{Unpacker, UnpackerWorker}
import uk.ac.wellcome.platform.archive.common.config.builders.OperationBuilder
import uk.ac.wellcome.platform.archive.common.ingests.models.UnpackBagRequest
import uk.ac.wellcome.storage.typesafe.S3Builder
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder

import scala.concurrent.ExecutionContext

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      AkkaBuilder.buildActorSystem()
    implicit val executionContext: ExecutionContext =
      AkkaBuilder.buildExecutionContext()
    implicit val materializer: ActorMaterializer =
      AkkaBuilder.buildActorMaterializer()

    implicit val s3Client =
      S3Builder.buildS3Client(config)

    new UnpackerWorker(
      config = UnpackerConfig(config),
      stream = NotificationStreamBuilder.buildStream[UnpackBagRequest](config),
      ingestUpdater = OperationBuilder.buildIngestUpdater(config, "unpacking"),
      outgoing = OperationBuilder.buildOutgoingPublisher(config, "unpacking"),
      reporter = OperationBuilder.buildOperationReporter(config),
      unpacker = new Unpacker()
    )
  }
}
