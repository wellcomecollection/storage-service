package uk.ac.wellcome.platform.archive.bagverifier.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.models.bagit.BagDigestFile
import uk.ac.wellcome.platform.archive.common.operation.Timed

case class VerificationSummary(
  successfulVerifications: Seq[BagDigestFile] = List.empty,
  failedVerifications: Seq[FailedVerification] = List.empty,
  startTime: Instant = Instant.now(),
  endTime: Option[Instant] = None)
    extends Timed {
  def succeeded: Boolean = failedVerifications.isEmpty
  def complete: VerificationSummary = this.copy(endTime = Some(Instant.now()))
}

case class FailedVerification(
  digestFile: BagDigestFile,
  reason: Throwable
)
