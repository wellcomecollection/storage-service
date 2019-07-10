package uk.ac.wellcome.platform.archive.bag_register.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.operation.models.Summary
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation

case class RegistrationSummary(
  bagRootLocation: ObjectLocation,
  storageSpace: StorageSpace,
  startTime: Instant,
  endTime: Option[Instant] = None
) extends Summary {
  def complete: RegistrationSummary =
    this.copy(
      endTime = Some(Instant.now())
    )

  override def toString: String =
    f"""|bag=$bagRootLocation
        |space=$storageSpace
        |durationSeconds=$durationSeconds
        |duration=$formatDuration""".stripMargin
      .replaceAll("\n", ", ")
}
