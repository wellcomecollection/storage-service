package weco.storage_service.bag_verifier.fixity
import weco.storage_service.verify.{Checksum, HashingAlgorithm}

sealed trait FailedChecksum

case class FailedChecksumCreation(algorithm: HashingAlgorithm, e: Throwable)
    extends Throwable(s"Could not create checksum: ${e.getMessage}")
    with FailedChecksum

case class FailedChecksumNoMatch(actual: Checksum, expected: Checksum)
    extends Throwable(
      s"Checksum values do not match! Expected: $expected, saw: $actual"
    )
    with FailedChecksum

case class FailedChecksumLocationNotFound[T](expected: ExpectedFileFixity)
    extends Throwable("VerifiableLocation not found!")
    with FailedChecksum
