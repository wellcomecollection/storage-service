package uk.ac.wellcome.platform.archive.common.operation

sealed trait OperationResult[T] {
  val summary: T
}

case class OperationCompleted[T](
  summary: T
) extends OperationResult[T]

case class OperationSuccess[T](
  summary: T
) extends OperationResult[T]

case class OperationFailure[T](
  summary: T,
  e: Throwable
) extends OperationResult[T]
