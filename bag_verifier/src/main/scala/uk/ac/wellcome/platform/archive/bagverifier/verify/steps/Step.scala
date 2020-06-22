package uk.ac.wellcome.platform.archive.bagverifier.verify.steps

trait Step {
  case class BagVerifierError(
    e: Throwable,
    userMessage: Option[String] = None
  )

  case object BagVerifierError {
    def apply(message: String): BagVerifierError =
      BagVerifierError(
        e = new Throwable(message),
        userMessage = Some(message)
      )
  }

  type InternalResult[T] = Either[BagVerifierError, T]
}
