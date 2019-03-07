package uk.ac.wellcome.platform.archive.common.operation

abstract class OperationResult[T] {
  val summary: T
  def copy(summary: T): OperationResult[T]
}

case class OperationCompleted[T](
  summary: T
) extends OperationResult[T] {
  def copy(summary: T) = OperationCompleted(summary)
}

case class OperationSuccess[T](
  summary: T
) extends OperationResult[T] {
  def copy(summary: T) = OperationSuccess(summary)
}

case class OperationFailure[T](
  summary: T,
  e: Throwable
) extends OperationResult[T] {
  def copy(summary: T) = OperationFailure(summary, e)
}
