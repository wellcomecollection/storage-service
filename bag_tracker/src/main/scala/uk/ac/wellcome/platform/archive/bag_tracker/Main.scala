package uk.ac.wellcome.platform.archive.bag_tracker

import akka.actor.ActorSystem
import com.typesafe.config.Config
import uk.ac.wellcome.platform.archive.common.config.builders.StorageManifestDaoBuilder
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      AkkaBuilder.buildActorSystem()

    // We need to bind to 0.0.0.0, not localhost, so the API listens for connections
    // from other services.
    //
    // If you bind to localhost, the API is only available to containers within
    // the same task definition in ECS.
    val host = "0.0.0.0"
    val port = 8080

    val storageManifestDao = StorageManifestDaoBuilder.build(config)

    new BagTrackerApi(storageManifestDao = storageManifestDao)(host = host, port = port)
  }
}
