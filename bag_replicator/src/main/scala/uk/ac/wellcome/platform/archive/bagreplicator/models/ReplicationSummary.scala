package uk.ac.wellcome.platform.archive.bagreplicator.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.BagLocation
import uk.ac.wellcome.platform.archive.common.operation.models.Summary

case class ReplicationSummary(
  startTime: Instant,
  endTime: Option[Instant] = None,
  source: BagLocation,
  destination: Option[BagLocation] = None,
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
    f"""|${source.completePath} -> $destinationCompletePath
        |replicated in $formatDuration
       """.stripMargin
      .replaceAll("\n", " ")
  }
}
