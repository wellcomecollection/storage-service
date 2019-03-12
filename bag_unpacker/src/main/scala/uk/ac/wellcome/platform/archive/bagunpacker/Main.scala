package uk.ac.wellcome.platform.archive.bagunpacker

import com.typesafe.config.Config
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.typesafe.NotificationStreamBuilder
import uk.ac.wellcome.platform.archive.bagunpacker.config.models.UnpackerConfig
import uk.ac.wellcome.platform.archive.bagunpacker.services.{
  Unpacker,
  UnpackerWorker
}
import uk.ac.wellcome.platform.archive.common.config.builders.OperationBuilder
import uk.ac.wellcome.platform.archive.common.models.UnpackBagRequest
import uk.ac.wellcome.storage.typesafe.S3Builder
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem =
      AkkaBuilder.buildActorSystem()

    implicit val s3Client =
      S3Builder.buildS3Client(config)

    implicit val ec = actorSystem.dispatcher

    new UnpackerWorker(
      config = UnpackerConfig(config),
      stream = NotificationStreamBuilder
        .buildStream[UnpackBagRequest](config),
      notifier = OperationBuilder.buildOperationNotifier(
        config,
        "unpacking"
      ),
      reporter = OperationBuilder.buildOperationReporter(config),
      unpacker = new Unpacker()
    )
  }
}
