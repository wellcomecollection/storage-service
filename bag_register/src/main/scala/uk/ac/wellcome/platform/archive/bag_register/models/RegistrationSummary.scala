package uk.ac.wellcome.platform.archive.bag_register.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.operation.models.Summary
import uk.ac.wellcome.platform.archive.common.storage.models.{
  PrimaryStorageLocation,
  StorageSpace
}

case class RegistrationSummary(
  ingestId: IngestID,
  location: PrimaryStorageLocation,
  space: StorageSpace,
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
      ("space", space)
    )
}
