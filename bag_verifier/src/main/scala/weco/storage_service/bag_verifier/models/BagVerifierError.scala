package weco.storage_service.bag_verifier.models

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
