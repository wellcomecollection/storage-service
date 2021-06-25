package weco.storage_service.bag_register.models

import java.time.Instant

import weco.storage_service.bagit.models.{
  BagVersion,
  ExternalIdentifier
}
import weco.storage_service.ingests.models.IngestID
import weco.storage_service.operation.models.Summary
import weco.storage_service.storage.models.{
  PrimaryReplicaLocation,
  StorageSpace
}

case class RegistrationSummary(
  ingestId: IngestID,
  location: PrimaryReplicaLocation,
  space: StorageSpace,
  externalIdentifier: ExternalIdentifier,
  version: BagVersion,
  startTime: Instant,
  maybeEndTime: Option[Instant] = None
) extends Summary {
  def complete: RegistrationSummary =
    this.copy(
      maybeEndTime = Some(Instant.now())
    )

  override val fieldsToLog: Seq[(String, Any)] =
    Seq(
      ("location", location),
      ("space", space),
      ("externalIdentifier", externalIdentifier),
      ("version", version)
    )
}
