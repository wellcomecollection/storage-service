package uk.ac.wellcome.platform.archive.bagreplicator.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.BagLocation
import uk.ac.wellcome.platform.archive.common.operation.models.Summary

trait ReplicationSummary extends Summary {
  val srcLocation: BagLocation
  val startTime: Instant
}

case class ReplicationStarted(
  srcLocation: BagLocation,
  startTime: Instant,
  endTime: Option[Instant] = None
) extends ReplicationSummary {
  def completedWith(dstLocation: BagLocation): ReplicationCompleted =
    ReplicationCompleted(
      srcLocation = srcLocation,
      dstLocation = dstLocation,
      startTime = startTime,
      endTime = Some(Instant.now())
    )

  def failed: ReplicationFailed =
    ReplicationFailed(
      srcLocation = srcLocation,
      startTime = startTime,
      endTime = Some(Instant.now())
    )

  override def toString: String =
    s"""|src=${srcLocation.completePath}
        |durationSeconds=$durationSeconds
        |duration=$formatDuration""".stripMargin
      .replaceAll("\n", ", ")
}

case class ReplicationCompleted(
  srcLocation: BagLocation,
  dstLocation: BagLocation,
  startTime: Instant,
  endTime: Option[Instant]
) extends ReplicationSummary {
  override def toString: String =
    s"""|src=${srcLocation.completePath}
        |dst=${dstLocation.completePath}
        |durationSeconds=$durationSeconds
        |duration=$formatDuration""".stripMargin
      .replaceAll("\n", ", ")
}

case class ReplicationFailed(
  srcLocation: BagLocation,
  startTime: Instant,
  endTime: Option[Instant]
) extends ReplicationSummary {
  override def toString: String =
    s"""|src=${srcLocation.completePath}
        |durationSeconds=$durationSeconds
        |duration=$formatDuration""".stripMargin
      .replaceAll("\n", ", ")
}
