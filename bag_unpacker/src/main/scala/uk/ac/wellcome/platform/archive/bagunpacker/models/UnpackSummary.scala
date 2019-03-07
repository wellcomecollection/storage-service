package uk.ac.wellcome.platform.archive.bagunpacker.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.operation.Timed

case class UnpackSummary(
  fileCount: Int = 0,
  bytesUnpacked: Long = 0L,
  startTime: Instant,
  endTime: Option[Instant] = None
) extends Timed {
  def complete: UnpackSummary =
    this.copy(endTime = Some(Instant.now()))

  override def toString() = {
    f"""|
        |
        |
     """.stripMargin
      .replaceAll("\n", " ")
  }
}
