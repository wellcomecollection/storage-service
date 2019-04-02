package uk.ac.wellcome.platform.archive.bagreplicator

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.Config
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.typesafe.NotificationStreamBuilder
import uk.ac.wellcome.platform.archive.bagreplicator.config.ReplicatorDestinationConfig
import uk.ac.wellcome.platform.archive.bagreplicator.services.{
  BagReplicator,
  BagReplicatorWorker
}
import uk.ac.wellcome.platform.archive.common.config.builders.{
  DiagnosticReporterBuilder,
  IngestUpdaterBuilder,
  OutgoingPublisherBuilder
}
import uk.ac.wellcome.platform.archive.common.ingests.models.BagRequest
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
    implicit val materializer: ActorMaterializer =
      AkkaBuilder.buildActorMaterializer()

    implicit val s3Client = S3Builder.buildS3Client(config)

    val operationName = "replicating"

    new BagReplicatorWorker(
      stream = NotificationStreamBuilder
        .buildStream[BagRequest](config),
      ingestUpdater = IngestUpdaterBuilder.build(config, operationName),
      outgoing = OutgoingPublisherBuilder.build(config, operationName),
      reporter = DiagnosticReporterBuilder.build(config),
      replicator = new BagReplicator(
        config = ReplicatorDestinationConfig
          .buildDestinationConfig(config),
        s3PrefixCopier = S3PrefixCopier(s3Client)
      )
    )
  }
}
