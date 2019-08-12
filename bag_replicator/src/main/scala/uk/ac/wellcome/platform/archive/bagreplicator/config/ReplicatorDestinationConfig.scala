package uk.ac.wellcome.platform.archive.bagreplicator.config

import com.typesafe.config.Config
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

case class ReplicatorDestinationConfig(
  namespace: String
)

case object ReplicatorDestinationConfig {
  def buildDestinationConfig(config: Config): ReplicatorDestinationConfig =
    ReplicatorDestinationConfig(
      namespace =
        config.required[String]("bag-replicator.storage.destination.bucket")
    )
}
