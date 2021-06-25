package weco.storage_service.operation.models

import java.time.{Duration, Instant}

trait Timed {
  val startTime: Instant
  val maybeEndTime: Option[Instant]

  def duration: Option[Duration] =
    maybeEndTime.map { Duration.between(startTime, _) }

  def durationSeconds: Long =
    duration.getOrElse(Duration.ofSeconds(0)).getSeconds
}
