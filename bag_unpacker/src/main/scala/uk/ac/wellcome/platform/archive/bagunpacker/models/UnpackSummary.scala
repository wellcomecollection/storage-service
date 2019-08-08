package uk.ac.wellcome.platform.archive.bagunpacker.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.operation.models.Summary
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}

case class UnpackSummary(
  id: IngestID,
  srcLocation: ObjectLocation,
  dstLocation: ObjectLocationPrefix,
  fileCount: Int = 0,
  bytesUnpacked: Long = 0L,
  startTime: Instant,
  maybeEndTime: Option[Instant] = None
) extends Summary {
  def complete: UnpackSummary =
    this.copy(maybeEndTime = Some(Instant.now()))
  override def toString: String = {
    f"""|id=$id
        |src=$srcLocation
        |dst=$dstLocation
        |files=$fileCount
        |bytesSize=$bytesUnpacked
        |size=${formatBytes(bytesUnpacked)}
        |durationSeconds=$durationSeconds
        |duration=$formatDuration""".stripMargin
      .replaceAll("\n", ", ")
  }
}
