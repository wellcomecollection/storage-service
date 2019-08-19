package uk.ac.wellcome.platform.archive.common.worker.models

import uk.ac.wellcome.platform.archive.common.storage.models.IngestStepResult

sealed trait Step[TraceIdentifier] {
  val id: TraceIdentifier
}

case class StepStarted[TraceIdentifier](
  id: TraceIdentifier
) extends Step[TraceIdentifier]

sealed trait StepResult[TraceIdentifier, +T] extends Step[TraceIdentifier] {
  val summary: T
  val maybeUserFacingMessage: Option[String]
}

case class StepSucceeded[TraceIdentifier, T](
    id: TraceIdentifier,
    summary: T,
    maybeUserFacingMessage: Option[String] = None
  ) extends StepResult[TraceIdentifier, T]


case class StepShouldRetry[TraceIdentifier, T](
  id: TraceIdentifier,
  summary: T,
  e: Throwable,
  maybeUserFacingMessage: Option[String] = None
) extends StepResult[TraceIdentifier, T]

sealed trait StepEndState[TraceIdentifier, T] extends StepResult[TraceIdentifier, T]

case class Completed[TraceIdentifier, T](
                                          id: TraceIdentifier,
                                          summary: T
                                        ) extends StepResult[TraceIdentifier, T] {
  override val maybeUserFacingMessage: Option[String] = None
}

case class Failed[T](
                            summary: T,
                            e: Throwable,
                            maybeUserFacingMessage: Option[String] = None
                          ) extends IngestStepResult[T]

