package uk.ac.wellcome.platform.archive.common.verify

trait Verifiable[T] {
  def create(t: T): List[VerifiableLocation]
}

object Verifiable {
  implicit def verifiable[T](
    implicit
      verifiable: T => List[VerifiableLocation]
      ) =
    new Verifiable[T] {
      override def create(t: T): List[VerifiableLocation] =
        verifiable(t)
    }
}