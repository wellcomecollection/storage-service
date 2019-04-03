package uk.ac.wellcome.platform.archive.bagreplicator.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.BagLocation
import uk.ac.wellcome.platform.archive.common.operation.models.Summary

trait ReplicationSummary extends Summary {
  val srcLocation: BagLocation
  val dstLocation: Option[BagLocation]
  val startTime: Instant
  val endTime: Option[Instant]
}

case class ReplicationResult(
  srcLocation: BagLocation,
  dstLocation: Option[BagLocation] = None,
  startTime: Instant,
  endTime: Option[Instant] = None,
) extends ReplicationSummary {
  def complete: ReplicationResult =
    this.copy(
      endTime = Some(Instant.now())
    )

  override def toString: String = {
    val destinationCompletePath = dstLocation match {
      case None                 => "<no-destination>"
      case Some(theDestination) => theDestination.completePath
    }
    f"""|src=${srcLocation.completePath}
        |dst=$destinationCompletePath
        |durationSeconds=$durationSeconds
        |duration=$formatDuration""".stripMargin
      .replaceAll("\n", ", ")
  }
}
