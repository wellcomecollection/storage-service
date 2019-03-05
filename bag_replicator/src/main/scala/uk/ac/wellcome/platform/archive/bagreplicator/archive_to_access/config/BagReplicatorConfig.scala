package uk.ac.wellcome.platform.archive.bagreplicator.config

import com.typesafe.config.Config
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

case class BagReplicatorConfig(
  destination: ReplicatorDestinationConfig
)

object BagReplicatorConfig {
  def buildBagReplicatorConfig(config: Config): BagReplicatorConfig = {
    BagReplicatorConfig(
      destination = ReplicatorDestinationConfig(
        namespace =
          config.required[String]("bag-replicator.storage.destination.bucket"),
        rootPath = config.get("bag-replicator.storage.destination.rootpath")
      )
    )
  }
}
