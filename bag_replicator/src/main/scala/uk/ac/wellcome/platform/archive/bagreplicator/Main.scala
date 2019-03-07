package uk.ac.wellcome.platform.archive.bagreplicator

import akka.actor.ActorSystem
import com.typesafe.config.Config
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.typesafe.NotificationStreamBuilder
import uk.ac.wellcome.platform.archive.bagreplicator.config.ReplicatorDestinationConfig
import uk.ac.wellcome.platform.archive.bagreplicator.services.{
  BagLocator,
  BagReplicator,
  BagReplicatorWorker
}
import uk.ac.wellcome.platform.archive.common.config.builders.OperationNotifierBuilder
import uk.ac.wellcome.platform.archive.common.models.BagRequest
import uk.ac.wellcome.storage.s3.S3PrefixCopier
import uk.ac.wellcome.storage.typesafe.S3Builder
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder

import scala.concurrent.ExecutionContextExecutor

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem = AkkaBuilder.buildActorSystem()

    implicit val executionContext: ExecutionContextExecutor =
      actorSystem.dispatcher

    val s3Client = S3Builder.buildS3Client(config)

    val operationName = "replicating"

    new BagReplicatorWorker(
      stream = NotificationStreamBuilder
        .buildStream[BagRequest](config),
      notifier = OperationNotifierBuilder
        .build(config, operationName),
      replicator = new BagReplicator(
        bagLocator = new BagLocator(s3Client),
        config = ReplicatorDestinationConfig
          .buildDestinationConfig(config),
        s3PrefixCopier = S3PrefixCopier(s3Client)
      )
    )
  }
}
