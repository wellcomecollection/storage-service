package uk.ac.wellcome.platform.archive.common.verify

trait Verifiable[T] {
  def create(t: T): List[VerifiableLocation]
}

object Verifiable {
  implicit class Convert[T](t: T)(implicit verifiable: Verifiable[T]) {
    def toVerifiable: List[VerifiableLocation] = {
      verifiable.create(t)
    }
  }
}
