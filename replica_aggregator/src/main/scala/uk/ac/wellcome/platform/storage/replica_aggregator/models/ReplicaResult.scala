package uk.ac.wellcome.platform.storage.replica_aggregator.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.EnrichedBagInformationPayload
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  InfrequentAccessStorageProvider,
  IngestID
}

case class ReplicaResult(
  ingestId: IngestID,
  storageLocation: BetterStorageLocation,
  timestamp: Instant
)

case object ReplicaResult {
  def apply(payload: EnrichedBagInformationPayload): ReplicaResult =
    ReplicaResult(
      ingestId = payload.ingestId,
      storageLocation = PrimaryStorageLocation(
        provider = InfrequentAccessStorageProvider,
        location = payload.bagRoot
      ),
      timestamp = Instant.now()
    )
}
