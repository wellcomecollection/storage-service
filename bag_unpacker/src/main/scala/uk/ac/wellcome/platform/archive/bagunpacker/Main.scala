package uk.ac.wellcome.platform.archive.bagunpacker

import akka.actor.ActorSystem
import com.typesafe.config.Config
import uk.ac.wellcome.messaging.sns.NotificationMessage
import uk.ac.wellcome.messaging.typesafe.{SNSBuilder, SQSBuilder}
import uk.ac.wellcome.platform.archive.bagunpacker.config.BagUnpackerConfig
import uk.ac.wellcome.storage.typesafe.S3Builder
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem = AkkaBuilder.buildActorSystem()

    new BagUnpacker(
      s3Client = S3Builder.buildS3Client(config),
      snsClient = SNSBuilder.buildSNSClient(config),
      sqsStream = SQSBuilder.buildSQSStream[NotificationMessage](config),
      bagUnpackerConfig = BagUnpackerConfig.buildBagUnpackerConfig(config),
      ingestsSnsConfig =
        SNSBuilder.buildSNSConfig(config, namespace = "ingests"),
      outgoingSnsConfig =
        SNSBuilder.buildSNSConfig(config, namespace = "outgoing")
    )
  }
}
