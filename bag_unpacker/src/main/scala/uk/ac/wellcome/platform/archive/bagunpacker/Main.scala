package uk.ac.wellcome.platform.archive.bagunpacker

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.amazonaws.services.s3.AmazonS3
import com.typesafe.config.Config
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.typesafe.NotificationStreamBuilder
import uk.ac.wellcome.platform.archive.bagunpacker.config.builders.UnpackerWorkerConfigBuilder
import uk.ac.wellcome.platform.archive.bagunpacker.services.{
  S3Uploader,
  Unpacker,
  UnpackerWorker
}
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

    implicit val s3Client: AmazonS3 =
      S3Builder.buildS3Client(config)

    new UnpackerWorker(
      config = UnpackerWorkerConfigBuilder.build(config),
      stream = NotificationStreamBuilder.buildStream[UnpackBagRequest](config),
      ingestUpdater = OperationBuilder.buildIngestUpdater(config, "unpacking"),
      outgoing = OperationBuilder.buildOutgoingPublisher(config, "unpacking"),
      reporter = OperationBuilder.buildOperationReporter(config),
      unpacker = Unpacker(
        s3Uploader = new S3Uploader()
      )
    )
  }
}
