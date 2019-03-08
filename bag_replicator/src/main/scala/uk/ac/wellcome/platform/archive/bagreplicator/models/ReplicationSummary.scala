package uk.ac.wellcome.platform.archive.bagreplicator.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.models.bagit.BagLocation
import uk.ac.wellcome.platform.archive.common.operation.Summary

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
    f"""|replicated in $describeDuration}
       """.stripMargin
      .replaceAll("\n", " ")
  }
}
