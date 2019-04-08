package uk.ac.wellcome.platform.archive.bag_register.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, BagLocation}
import uk.ac.wellcome.platform.archive.common.operation.models.Summary

case class RegistrationSummary(
  location: BagLocation,
  bagId: Option[BagId] = None,
  startTime: Instant,
  endTime: Option[Instant] = None
) extends Summary {
  def complete: RegistrationSummary =
    this.copy(
      endTime = Some(Instant.now())
    )

  override def toString: String =
    f"""|bag=${location.completePath}
        |id=${bagId.getOrElse("<unknown-bag>")}
        |durationSeconds=$durationSeconds
        |duration=$formatDuration""".stripMargin
      .replaceAll("\n", ", ")
}
