package uk.ac.wellcome.platform.archive.common.operation.models

sealed trait WorkResult[T] {
  val summary: T
  def withSummary(summary: T): WorkResult[T]
}

case class WorkSucceeded[T](
  summary: T
) extends WorkResult[T] {
  def withSummary(summary: T) = WorkSucceeded(summary)
}

case class WorkFailed[T](
  summary: T,
  e: Throwable
) extends WorkResult[T] {
  def withSummary(summary: T) = WorkFailed(summary, e)
}
