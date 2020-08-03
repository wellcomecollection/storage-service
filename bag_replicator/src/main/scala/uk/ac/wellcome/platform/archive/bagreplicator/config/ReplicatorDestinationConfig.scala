package uk.ac.wellcome.platform.archive.bagreplicator.config

import com.typesafe.config.Config
import uk.ac.wellcome.platform.archive.bagreplicator.models._
import uk.ac.wellcome.platform.archive.common.ingests.models.StorageProvider
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

case class ReplicatorDestinationConfig(
  namespace: String,
  provider: StorageProvider,
  replicaType: ReplicaType
)

case object ReplicatorDestinationConfig {
  def buildDestinationConfig(config: Config): ReplicatorDestinationConfig = {
    val replicaTypeString = config.requireString("bag-replicator.replicaType")

    val replicaType: ReplicaType =
      replicaTypeString match {
        case "primary"   => PrimaryReplica
        case "secondary" => SecondaryReplica
        case _ =>
          throw new IllegalArgumentException(
            s"Unrecognised replica type: $replicaTypeString, expected primary/secondary"
          )
      }

    ReplicatorDestinationConfig(
      namespace =
        config.requireString("bag-replicator.storage.destination.namespace"),
      provider = StorageProvider(
        config.requireString("bag-replicator.provider")
      ),
      replicaType = replicaType
    )
  }
}
