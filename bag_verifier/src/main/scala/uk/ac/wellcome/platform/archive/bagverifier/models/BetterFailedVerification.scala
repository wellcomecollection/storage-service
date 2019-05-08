package uk.ac.wellcome.platform.archive.bagverifier.models

case class BetterFailedVerification(
  request: VerificationRequest,
  error: Throwable
)
