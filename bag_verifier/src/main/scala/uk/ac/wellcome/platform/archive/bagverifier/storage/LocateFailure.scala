package uk.ac.wellcome.platform.archive.bagverifier.storage

sealed trait LocateFailure[T] {
  val t: T
  val msg: String
}

// TODO: Check these error
case class LocationError[T](t: T, msg: String)
  extends Throwable(msg)
    with LocateFailure[T]
case class LocationNotFound[T](t: T, msg: String)
  extends Throwable(msg)
    with LocateFailure[T]
case class LocationParsingError[T](t: T, msg: String)
  extends Throwable(msg)
    with LocateFailure[T]
