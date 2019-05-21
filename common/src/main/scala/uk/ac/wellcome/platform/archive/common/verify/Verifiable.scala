package uk.ac.wellcome.platform.archive.common.verify

trait Verifiable[T] {
  def create(t: T): Either[VerifiableGenerationFailure, List[VerifiableLocation]]
}

object Verifiable {
  implicit class Convert[T](t: T)(implicit verifiable: Verifiable[T]) {
    def toVerifiable: Either[VerifiableGenerationFailure, List[VerifiableLocation]] = {
      verifiable.create(t)
    }
  }
}

sealed trait VerifiableGenerationFailure {
  val msg: String
}

case class VerifiableGenerationFailed(msg: String) extends Throwable(msg) with VerifiableGenerationFailure