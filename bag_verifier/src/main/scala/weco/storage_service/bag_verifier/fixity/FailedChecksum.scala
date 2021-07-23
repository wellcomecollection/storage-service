package weco.storage_service.bag_verifier.fixity

import weco.storage_service.checksum.{ChecksumAlgorithm, MismatchedChecksum}

sealed trait FailedChecksum

case class FailedChecksumCreation(algorithm: ChecksumAlgorithm, e: Throwable)
    extends Throwable(s"Could not create checksum: ${e.getMessage}")
    with FailedChecksum

case class FailedChecksumNoMatch(mismatches: Seq[MismatchedChecksum])
    extends Throwable(
      s"Checksum values do not match: ${mismatches.map(_.description).mkString("; ")}."
    )
    with FailedChecksum

case class FailedChecksumLocationNotFound[T](expected: ExpectedFileFixity)
    extends Throwable("VerifiableLocation not found!")
    with FailedChecksum
