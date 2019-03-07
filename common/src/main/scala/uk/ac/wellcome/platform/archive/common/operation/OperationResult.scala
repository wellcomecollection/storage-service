package uk.ac.wellcome.platform.archive.common.operation

abstract class OperationResult[T] {
  val summary: T
  def withSummary(summary: T): OperationResult[T]
}

case class OperationCompleted[T](
  summary: T
) extends OperationResult[T] {
  def withSummary(summary: T) = OperationCompleted(summary)
}

case class OperationSuccess[T](
  summary: T
) extends OperationResult[T] {
  def withSummary(summary: T) = OperationSuccess(summary)
}

case class OperationFailure[T](
  summary: T,
  e: Throwable
) extends OperationResult[T] {
  def withSummary(summary: T) = OperationFailure(summary, e)
}
