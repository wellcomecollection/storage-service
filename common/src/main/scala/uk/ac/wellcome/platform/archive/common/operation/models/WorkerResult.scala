package uk.ac.wellcome.platform.archive.common.operation.models

sealed trait WorkerResult[T] {
  val summary: T
  def withSummary(summary: T): WorkerResult[T]
}

case class WorkerSucceeded[T](
  summary: T
) extends WorkerResult[T] {
  def withSummary(summary: T) = WorkerSucceeded(summary)
}

case class WorkerFailed[T](
  summary: T,
  e: Throwable
) extends WorkerResult[T] {
  def withSummary(summary: T) = WorkerFailed(summary, e)
}
