package uk.ac.wellcome.platform.archive.bagunpacker.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.operation.models.IdentifiedSummary
import uk.ac.wellcome.storage.ObjectLocation

case class UnpackSummary(id: String,
                         srcLocation: ObjectLocation,
                         dstLocation: ObjectLocation,
                         fileCount: Int = 0,
                         bytesUnpacked: Long = 0L,
                         startTime: Instant,
                         endTime: Option[Instant] = None)
    extends IdentifiedSummary {
  def complete: UnpackSummary =
    this.copy(endTime = Some(Instant.now()))
  override def toString(): String = {
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
