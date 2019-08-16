package uk.ac.wellcome.platform.archive.bagreplicator.replicator.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.operation.models.Summary
import uk.ac.wellcome.storage.ObjectLocationPrefix

case class ReplicationSummary(
  startTime: Instant,
  maybeEndTime: Option[Instant] = None,
  request: ReplicationRequest) extends Summary {

  def srcPrefix: ObjectLocationPrefix = request.srcPrefix
  def dstPrefix: ObjectLocationPrefix = request.dstPrefix

  override def toString: String =
    f"""|src=$srcPrefix
        |dst=$dstPrefix
        |durationSeconds=$durationSeconds
        |duration=$formatDuration""".stripMargin
      .replaceAll("\n", ", ")

  def complete: ReplicationSummary = this.copy(
    maybeEndTime = Some(Instant.now)
  )
}
