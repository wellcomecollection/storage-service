package uk.ac.wellcome.platform.archive.bag_register.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.operation.models.Summary
import uk.ac.wellcome.platform.archive.common.storage.models.{
  PrimaryStorageLocation,
  StorageSpace
}

case class RegistrationSummary(
  location: PrimaryStorageLocation,
  space: StorageSpace,
  startTime: Instant,
  maybeEndTime: Option[Instant] = None
) extends Summary {
  def complete: RegistrationSummary =
    this.copy(
      maybeEndTime = Some(Instant.now())
    )

  override def toString: String =
    f"""|bag=$location
        |space=$space
        |durationSeconds=$durationSeconds
        |duration=$formatDuration""".stripMargin
      .replaceAll("\n", ", ")
}
