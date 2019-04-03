package uk.ac.wellcome.platform.archive.common.operation.models

import java.time.{Duration, Instant}

trait Timed {
  val startTime: Instant
  val endTime: Option[Instant]

  def duration =
    endTime.map(Duration.between(startTime, _))

  def durationSeconds =
    duration.getOrElse(Duration.ofSeconds(0)).getSeconds
}
