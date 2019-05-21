package uk.ac.wellcome.platform.archive.common.storage

import uk.ac.wellcome.storage.ObjectLocation

trait Locatable[T] {
  def locate(t: T)(maybeRoot: Option[ObjectLocation]): Either[LocateFailure[T], ObjectLocation]
}

object Locatable {

  implicit class LocatableOps[T](t: T)(
    implicit locator: Locatable[T]
  ) {
    def locateWith(root: ObjectLocation): Either[LocateFailure[T], ObjectLocation] =
      locator.locate(t)(Some(root))

    def locate: Either[LocateFailure[T], ObjectLocation] =
      locator.locate(t)(None)
  }
}

sealed trait LocateFailure[T] {
  val t: T
  val msg: String
}

case class LocationNotFound[T](t: T, msg: String) extends Throwable(msg) with LocateFailure[T]
case class LocationParsingError[T](t: T, msg: String) extends Throwable(msg) with LocateFailure[T]
