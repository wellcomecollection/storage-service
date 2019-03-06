package uk.ac.wellcome.platform.archive.common.operation

import java.time.{Duration, Instant}

trait Timed {
  val startTime: Instant
  val endTime: Option[Instant]

  def duration = {
    endTime.map(Duration.between(startTime, _))
  }
}
