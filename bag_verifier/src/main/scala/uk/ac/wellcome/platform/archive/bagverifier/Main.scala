package uk.ac.wellcome.platform.archive.bagverifier

import akka.actor.ActorSystem
import com.typesafe.config.Config
import uk.ac.wellcome.messaging.typesafe.SNSBuilder
import uk.ac.wellcome.platform.archive.bagreplicator.config.BagVerifierConfig
import uk.ac.wellcome.storage.typesafe.S3Builder
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem = AkkaBuilder.buildActorSystem()

    new BagVerifier(
      s3Client = S3Builder.buildS3Client(config),
      snsClient = SNSBuilder.buildSNSClient(config),

      bagVerifierConfig =
        BagVerifierConfig.buildBagVerifierConfig(config),

      progressSnsConfig =
        SNSBuilder.buildSNSConfig(config, namespace = "progress"),

      outgoingSnsConfig =
        SNSBuilder.buildSNSConfig(config, namespace = "outgoing")
    )
  }
}
