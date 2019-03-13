package uk.ac.wellcome.platform.archive.bagreplicator.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.models.bagit.BagLocation
import uk.ac.wellcome.platform.archive.common.operation.Summary

case class ReplicationSummary(
  source: BagLocation,
  destination: Option[BagLocation] = None,
  startTime: Instant,
  endTime: Option[Instant] = None,
) extends Summary {
  def complete: ReplicationSummary = {
    this.copy(
      endTime = Some(Instant.now())
    )
  }
  override def toString(): String = {
    val destinationCompletePath = destination match {
      case None                 => "<no-destination>"
      case Some(theDestination) => theDestination.completePath
    }
    f"""|src=${source.completePath}
        |dst=${destinationCompletePath}
        |duration=$formatDuration""".stripMargin
      .replaceAll("\n", ", ")
  }
}
