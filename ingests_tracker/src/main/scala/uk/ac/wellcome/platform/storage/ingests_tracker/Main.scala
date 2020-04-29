package uk.ac.wellcome.platform.storage.ingests_tracker

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.typesafe.config.Config
import uk.ac.wellcome.platform.archive.common.ingests.tracker.IngestTracker
import uk.ac.wellcome.platform.archive.common.ingests.tracker.dynamo.DynamoIngestTracker
import uk.ac.wellcome.storage.typesafe.DynamoBuilder
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder


object Main extends WellcomeTypesafeApp {
    runWithConfig { config: Config =>
      implicit val actorSystem: ActorSystem =
        AkkaBuilder.buildActorSystem()
      implicit val materializer: ActorMaterializer =
        AkkaBuilder.buildActorMaterializer()

      implicit val dynamoClient: AmazonDynamoDB =
        DynamoBuilder.buildDynamoClient(config)

      new IngestsTrackerApi {
        override val ingestTracker: IngestTracker = new DynamoIngestTracker(
          config = DynamoBuilder.buildDynamoConfig(config)
        )
        override implicit protected val sys: ActorSystem = actorSystem
        override implicit protected val mat: ActorMaterializer = materializer
      }
    }
}