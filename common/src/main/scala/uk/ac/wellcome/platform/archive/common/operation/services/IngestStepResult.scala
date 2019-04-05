package uk.ac.wellcome.platform.archive.common.operation.services

import uk.ac.wellcome.messaging.worker.models.{DeterministicFailure, Result, Successful}

sealed trait IngestStepResult[T] {
  val summary: T
  def withSummary(summary: T): IngestStepResult[T]
}

case class IngestCompleted[T](
  summary: T
) extends IngestStepResult[T] {
  def withSummary(summary: T) = IngestCompleted(summary)
}

case class IngestStepSuccess[T](
  summary: T
) extends IngestStepResult[T] {
  def withSummary(summary: T) = IngestStepSuccess(summary)
}

case class IngestFailed[T](
  summary: T,
  e: Throwable
) extends IngestStepResult[T] {
  def withSummary(summary: T) = IngestFailed(summary, e)
}

trait IngestStepWorker {
  def toResult[T](ingestResult: IngestStepResult[T]): Result[T] =
    ingestResult match {
      case IngestStepSuccess(s) => Successful(Some(s))
      case IngestCompleted(s)   => Successful(Some(s))
      case IngestFailed(s, t)   => DeterministicFailure(t, Some(s))
    }
}
