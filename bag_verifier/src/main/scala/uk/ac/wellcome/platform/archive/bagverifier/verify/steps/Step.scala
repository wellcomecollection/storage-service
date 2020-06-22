package uk.ac.wellcome.platform.archive.bagverifier.verify.steps

trait Step {
  case class BagVerifierError(
    e: Throwable,
    userMessage: Option[String] = None
  )

  type InternalResult[T] = Either[BagVerifierError, T]
}
