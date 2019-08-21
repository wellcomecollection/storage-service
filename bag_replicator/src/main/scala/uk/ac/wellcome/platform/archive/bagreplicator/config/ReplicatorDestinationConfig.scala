package uk.ac.wellcome.platform.archive.bagreplicator.config

import com.typesafe.config.Config
import uk.ac.wellcome.platform.archive.common.ingests.models.StorageProvider
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

case class ReplicatorDestinationConfig(
  namespace: String,
  provider: StorageProvider
)

case object ReplicatorDestinationConfig {
  def buildDestinationConfig(config: Config): ReplicatorDestinationConfig =
    ReplicatorDestinationConfig(
      namespace =
        config.required[String]("bag-replicator.storage.destination.bucket"),
      provider = StorageProvider(
        config.required[String]("bag-replicator.provider")
      )
    )
}
