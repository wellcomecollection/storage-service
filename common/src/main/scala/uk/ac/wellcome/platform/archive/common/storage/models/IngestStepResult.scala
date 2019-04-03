package uk.ac.wellcome.platform.archive.common.storage.models

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
                            e: Throwable,
                            maybeUserFacingMessage: Option[String] = None
) extends IngestStepResult[T] {
  def withSummary(summary: T) = IngestFailed(summary, e, maybeUserFacingMessage)
}
