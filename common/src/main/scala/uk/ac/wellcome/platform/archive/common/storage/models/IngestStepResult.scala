package uk.ac.wellcome.platform.archive.common.storage.models

import uk.ac.wellcome.messaging.worker.models.{
  DeterministicFailure,
  Result,
  Successful
}

sealed trait IngestStepResult[T] {
  val summary: T
  def withSummary(summary: T): IngestStepResult[T]
}

case class IngestCompleted[T](
  summary: T
) extends IngestStepResult[T] {
  def withSummary(summary: T) = IngestCompleted(summary)
}

case class IngestStepSucceeded[T](
  summary: T
) extends IngestStepResult[T] {
  def withSummary(summary: T) = IngestStepSucceeded(summary)
}

case class IngestFailed[T](
  summary: T,
  e: Throwable,
  maybeUserFacingMessage: Option[String] = None
) extends IngestStepResult[T] {
  def withSummary(summary: T) = IngestFailed(summary, e, maybeUserFacingMessage)
}

trait IngestStepWorker {
  def toResult[T](ingestResult: IngestStepResult[T]): Result[T] =
    ingestResult match {
      case IngestStepSucceeded(s) => Successful(Some(s))
      case IngestCompleted(s)     => Successful(Some(s))
      case IngestFailed(s, t, _)  => DeterministicFailure(t, Some(s))
    }
}
