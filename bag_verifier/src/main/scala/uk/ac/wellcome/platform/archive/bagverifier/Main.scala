package uk.ac.wellcome.platform.archive.bagverifier

import akka.actor.ActorSystem
import com.typesafe.config.Config
import uk.ac.wellcome.messaging.sns.NotificationMessage
import uk.ac.wellcome.messaging.typesafe.{SNSBuilder, SQSBuilder}
import uk.ac.wellcome.platform.archive.bagverifier.config.BagVerifierConfig
import uk.ac.wellcome.storage.typesafe.S3Builder
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder

import scala.concurrent.ExecutionContext

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem = AkkaBuilder.buildActorSystem()
    implicit val executionContext: ExecutionContext =
      AkkaBuilder.buildExecutionContext()

    new BagVerifier(
      s3Client = S3Builder.buildS3Client(config),
      snsClient = SNSBuilder.buildSNSClient(config),
      sqsStream = SQSBuilder.buildSQSStream[NotificationMessage](config),
      bagVerifierConfig = BagVerifierConfig.buildBagVerifierConfig(config),
      ingestsSnsConfig =
        SNSBuilder.buildSNSConfig(config, namespace = "progress"),
      outgoingSnsConfig =
        SNSBuilder.buildSNSConfig(config, namespace = "outgoing")
    )
  }
}
