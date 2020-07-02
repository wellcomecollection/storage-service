package uk.ac.wellcome.platform.archive.bagverifier.storage

trait Locatable[LocationResult, SearchRoot, T] {
  def locate(t: T)(
    maybeRoot: Option[SearchRoot]
  ): Either[LocateFailure[T], LocationResult]
}

object Locatable {
  implicit class LocatableOps[LocationResult, SearchRoot, T](t: T)(
    implicit locator: Locatable[LocationResult, SearchRoot, T]
  ) {
    def locateWith(
      root: SearchRoot
    ): Either[LocateFailure[T], LocationResult] =
      locator.locate(t)(Some(root))

    def locate: Either[LocateFailure[T], LocationResult] =
      locator.locate(t)(None)
  }
}
