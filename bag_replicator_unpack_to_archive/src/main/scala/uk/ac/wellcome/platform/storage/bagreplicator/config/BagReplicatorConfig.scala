package uk.ac.wellcome.platform.storage.bagreplicator.config

import com.typesafe.config.Config
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

case class BagReplicatorConfig(
  parallelism: Int,
  destination: ReplicatorDestinationConfig
)

object BagReplicatorConfig {
  def buildBagReplicatorConfig(config: Config): BagReplicatorConfig = {
    BagReplicatorConfig(
      parallelism =
        config.getOrElse[Int]("bag-replicator.parallelism")(default = 10),
      destination = ReplicatorDestinationConfig(
        namespace =
          config.required[String]("bag-replicator.storage.destination.bucket"),
        rootPath = config.get("bag-replicator.storage.destination.rootpath")
      )
    )
  }
}
