package uk.ac.wellcome.platform.archive.bagunpacker

import java.nio.file.Paths
import java.util.UUID

import akka.actor.ActorSystem
import com.amazonaws.services.s3.AmazonS3
import com.typesafe.config.Config
import uk.ac.wellcome.messaging.typesafe.{NotificationStreamBuilder, SNSBuilder}
import uk.ac.wellcome.platform.archive.bagunpacker.services.{BagUnpackerWorkerService, UnpackerService}
import uk.ac.wellcome.platform.archive.common.models.UnpackBagRequest
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.typesafe.S3Builder
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem = AkkaBuilder.buildActorSystem()

    implicit val s3Client: AmazonS3 = S3Builder.buildS3Client(config)

    implicit val ec = actorSystem.dispatcher

    val namespace = config.required[String]("destination.namespace")
    val prefix = config.required[String]("destination.prefix")

    new BagUnpackerWorkerService(
      bagUnpackerConfig = BagUnpackerConfig(namespace, prefix),
      stream = NotificationStreamBuilder.buildStream[UnpackBagRequest](config),
      progressSnsWriter =
        SNSBuilder.buildSNSWriter(config, namespace = "progress"),
      outgoingSnsWriter =
        SNSBuilder.buildSNSWriter(config, namespace = "outgoing"),
      unpackerService =
        new UnpackerService()
    )
  }
}

case class BagUnpackerConfig(
                                         namespace: String,
                                         prefix: String
                                         )


