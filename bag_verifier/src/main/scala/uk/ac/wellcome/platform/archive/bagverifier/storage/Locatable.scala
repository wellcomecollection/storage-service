package uk.ac.wellcome.platform.archive.bagverifier.storage

trait Locatable[LocationResult, T] {
  def locate(t: T)(
    maybeRoot: Option[LocationResult]
  ): Either[LocateFailure[T], LocationResult]
}

object Locatable {
  implicit class LocatableOps[LocationResult, T](t: T)(
    implicit locator: Locatable[LocationResult, T]
  ) {
    def locateWith(
      root: LocationResult
    ): Either[LocateFailure[T], LocationResult] =
      locator.locate(t)(Some(root))

    def locate: Either[LocateFailure[T], LocationResult] =
      locator.locate(t)(None)
  }
}

