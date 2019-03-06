package uk.ac.wellcome.platform.archive.bagreplicator.config

import com.typesafe.config.Config
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

// Specifies the S3 bucket and root path for objects copied by the replicator.
case class ReplicatorDestinationConfig(
  namespace: String,
  rootPath: Option[String]
)

case object ReplicatorDestinationConfig {
  def buildDestinationConfig(config: Config): ReplicatorDestinationConfig =
    ReplicatorDestinationConfig(
      namespace =
        config.required[String]("bag-replicator.storage.destination.bucket"),
      rootPath = config.get("bag-replicator.storage.destination.rootpath")
    )
}
