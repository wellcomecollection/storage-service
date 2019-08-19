package uk.ac.wellcome.platform.storage.replica_aggregator.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.EnrichedBagInformationPayload
import uk.ac.wellcome.platform.archive.common.ingests.models.InfrequentAccessStorageProvider
import uk.ac.wellcome.platform.archive.common.storage.models.{PrimaryStorageLocation, SecondaryStorageLocation, StorageLocation}

case class ReplicaResult(
  storageLocation: StorageLocation,
  timestamp: Instant
)

case object ReplicaResult {
  def apply(payload: EnrichedBagInformationPayload): ReplicaResult =
    ReplicaResult(
      storageLocation = PrimaryStorageLocation(
        provider = InfrequentAccessStorageProvider,
        prefix = payload.bagRootLocation.asPrefix
      ),
      timestamp = Instant.now()
    )
}



// TODO: This needs moving somewhere!
case class KnownReplicas(
                          location: PrimaryStorageLocation,
                          replicas: List[SecondaryStorageLocation]
                        )