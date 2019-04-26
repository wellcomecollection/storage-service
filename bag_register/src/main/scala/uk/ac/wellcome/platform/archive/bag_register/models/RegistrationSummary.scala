package uk.ac.wellcome.platform.archive.bag_register.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.operation.models.Summary
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation

case class RegistrationSummary(
  bagRootLocation: ObjectLocation,
  storageSpace: StorageSpace,
  bagId: Option[BagId] = None,
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
        |id=${bagId.getOrElse("<unknown-bag>")}
        |durationSeconds=$durationSeconds
        |duration=$formatDuration""".stripMargin
      .replaceAll("\n", ", ")
}
