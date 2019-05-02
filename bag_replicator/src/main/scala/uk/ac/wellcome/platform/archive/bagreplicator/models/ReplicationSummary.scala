package uk.ac.wellcome.platform.archive.bagreplicator.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.operation.models.Summary
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation

case class ReplicationSummary(
  bagRootLocation: ObjectLocation,
  storageSpace: StorageSpace,
  destination: ObjectLocation,
  startTime: Instant,
  endTime: Option[Instant] = None,
) extends Summary {
  def complete: ReplicationSummary = {
    this.copy(
      endTime = Some(Instant.now())
    )
  }

  override def toString: String =
    f"""|src=$bagRootLocation
        |dst=$destination
        |durationSeconds=$durationSeconds
        |duration=$formatDuration""".stripMargin
      .replaceAll("\n", ", ")
}
