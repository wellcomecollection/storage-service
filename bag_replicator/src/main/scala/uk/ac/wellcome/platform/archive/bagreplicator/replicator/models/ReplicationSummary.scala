package uk.ac.wellcome.platform.archive.bagreplicator.replicator.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.operation.models.Summary

case class ReplicationSummary(
  startTime: Instant,
  maybeEndTime: Option[Instant] = None,
  request: ReplicationRequest) extends Summary {

  override def toString: String =
    f"""|src=${request.srcPrefix}
        |dst=${request.dstPrefix}
        |durationSeconds=$durationSeconds
        |duration=$formatDuration""".stripMargin
      .replaceAll("\n", ", ")

  def complete: ReplicationSummary = this.copy(
    maybeEndTime = Some(Instant.now)
  )
}
