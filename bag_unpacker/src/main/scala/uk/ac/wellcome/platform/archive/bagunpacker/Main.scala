package uk.ac.wellcome.platform.archive.bagunpacker

import com.typesafe.config.Config
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.typesafe.NotificationStreamBuilder
import uk.ac.wellcome.platform.archive.bagunpacker.config.builders.OperationNotifierBuilder
import uk.ac.wellcome.platform.archive.bagunpacker.config.models.BagUnpackerConfig
import uk.ac.wellcome.platform.archive.bagunpacker.services.{
  BagUnpackerWorker,
  Unpacker
}
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

    new BagUnpackerWorker(
      config = BagUnpackerConfig(config),
      stream = NotificationStreamBuilder
        .buildStream[UnpackBagRequest](config),
      notifier = OperationNotifierBuilder.build(
        config,
        "unpacking"
      ),
      unpacker = new Unpacker()
    )
  }
}
