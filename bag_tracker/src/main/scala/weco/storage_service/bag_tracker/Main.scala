package weco.storage_service.bag_tracker

import org.apache.pekko.actor.ActorSystem
import com.typesafe.config.Config
import weco.storage_service.bag_tracker.config.builders.StorageManifestDaoBuilder
import weco.typesafe.WellcomeTypesafeApp

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      ActorSystem("main-actor-system")

    // We need to bind to 0.0.0.0, not localhost, so the API listens for connections
    // from other services.
    //
    // If you bind to localhost, the API is only available to containers within
    // the same task definition in ECS.
    val host = "0.0.0.0"
    val port = 8080

    val storageManifestDao = StorageManifestDaoBuilder.build(config)

    new BagTrackerApi(storageManifestDao = storageManifestDao)(
      host = host,
      port = port
    )
  }
}
