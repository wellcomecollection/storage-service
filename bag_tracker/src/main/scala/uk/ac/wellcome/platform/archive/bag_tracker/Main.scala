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

    new BagTrackerApi(
      storageManifestDao = StorageManifestDaoBuilder.build(config)
    )
  }
}
