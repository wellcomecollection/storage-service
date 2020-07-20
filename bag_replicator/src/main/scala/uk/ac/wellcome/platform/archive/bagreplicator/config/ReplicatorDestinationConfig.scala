package uk.ac.wellcome.platform.archive.bagreplicator.config

import com.typesafe.config.Config
import uk.ac.wellcome.platform.archive.bagreplicator.models.{
  BagReplicationRequest,
  PrimaryBagReplicationRequest,
  SecondaryBagReplicationRequest
}
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.models.ReplicationRequest
import uk.ac.wellcome.platform.archive.common.ingests.models.StorageProvider
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

case class ReplicatorDestinationConfig(
  namespace: String,
  provider: StorageProvider,
  requestBuilder: ReplicationRequest => BagReplicationRequest
)

case object ReplicatorDestinationConfig {
  def buildDestinationConfig(config: Config): ReplicatorDestinationConfig = {
    val replicaType = config.requireString("bag-replicator.replicaType")

    val requestBuilder: (ReplicationRequest => BagReplicationRequest) =
      replicaType match {
        case "primary"   => PrimaryBagReplicationRequest.apply
        case "secondary" => SecondaryBagReplicationRequest.apply
        case _ =>
          throw new IllegalArgumentException(
            s"Unrecognised replica type: $replicaType, expected primary/secondary"
          )
      }

    ReplicatorDestinationConfig(
      namespace =
        config.requireString("bag-replicator.storage.destination.bucket"),
      provider = StorageProvider(
        config.requireString("bag-replicator.provider")
      ),
      requestBuilder = requestBuilder
    )
  }
}
