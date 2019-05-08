package uk.ac.wellcome.platform.archive.bagverifier.models

case class FailedVerification(
  request: VerificationRequest,
  error: Throwable
)
