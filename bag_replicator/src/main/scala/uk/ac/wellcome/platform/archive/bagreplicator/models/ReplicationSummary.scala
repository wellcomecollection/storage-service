package uk.ac.wellcome.platform.archive.bagreplicator.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.BagLocation
import uk.ac.wellcome.platform.archive.common.operation.models.Summary

case class ReplicationSummary(
  source: BagLocation,
  maybeDestination: Option[BagLocation] = None,
  startTime: Instant,
  endTime: Option[Instant] = None,
) extends Summary {
  def complete: ReplicationSummary = {
    this.copy(
      endTime = Some(Instant.now())
    )
  }

  def destination: BagLocation =
    maybeDestination.getOrElse(
      throw new RuntimeException(
        "No destination provided by replication!"
      )
    )

  override def toString: String = {
    val destinationCompletePath = maybeDestination match {
      case None              => "<no-destination>"
      case Some(destination) => destination.completePath
    }
    f"""|src=${source.completePath}
        |dst=$destinationCompletePath
        |durationSeconds=$durationSeconds
        |duration=$formatDuration""".stripMargin
      .replaceAll("\n", ", ")
  }
}
