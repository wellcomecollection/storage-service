package uk.ac.wellcome.platform.archive.bag_register.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.operation.models.Summary
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocationPrefix

case class RegistrationSummary(
  bagRoot: ObjectLocationPrefix,
  space: StorageSpace,
  startTime: Instant,
  maybeEndTime: Option[Instant] = None
) extends Summary {
  def complete: RegistrationSummary =
    this.copy(
      maybeEndTime = Some(Instant.now())
    )

  override def toString: String =
    f"""|bag=$bagRoot
        |space=$space
        |durationSeconds=$durationSeconds
        |duration=$formatDuration""".stripMargin
      .replaceAll("\n", ", ")
}
