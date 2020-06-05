package uk.ac.wellcome.platform.archive.common.verify

trait Verifiable[T] {
  def create(t: T): Either[VerifiableGenerationFailed, Seq[VerifiableLocation]]
}

case class VerifiableGenerationFailed(msg: String)
    extends Throwable(msg)
