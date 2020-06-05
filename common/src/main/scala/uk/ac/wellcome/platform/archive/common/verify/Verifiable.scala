package uk.ac.wellcome.platform.archive.common.verify

trait Verifiable[T] {
  def create(t: T): Either[VerifiableGenerationFailure, Seq[VerifiableLocation]]
}

sealed trait VerifiableGenerationFailure {
  val msg: String
}

case class VerifiableGenerationFailed(msg: String)
    extends Throwable(msg)
    with VerifiableGenerationFailure
