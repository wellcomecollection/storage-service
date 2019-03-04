package uk.ac.wellcome.platform.archive.bagunpacker.models

import java.time.{Duration, Instant}


case class UnpackSummary(
                         fileCount: Int = 0,
                         bytesUnpacked: Long = 0L,
                         startTime: Instant,
                         endTime: Option[Instant] = None
                       ) extends Timed {
  def complete: UnpackSummary = this.copy(endTime = Some(Instant.now()))
}

trait Timed {
  val startTime: Instant
  val endTime: Option[Instant]

  def duration = {
    endTime.map(Duration.between(startTime, _))
  }
}