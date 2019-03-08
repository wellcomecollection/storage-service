package uk.ac.wellcome.platform.archive.bagverifier.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.models.bagit.BagDigestFile
import uk.ac.wellcome.platform.archive.common.operation.Summary

case class VerificationSummary(
  successfulVerifications: Seq[BagDigestFile] = List.empty,
  failedVerifications: Seq[FailedVerification] = List.empty,
  startTime: Instant = Instant.now(),
  endTime: Option[Instant] = None)
    extends Summary {
  def succeeded: Boolean = failedVerifications.isEmpty
  def complete: VerificationSummary = this.copy(endTime = Some(Instant.now()))

  override def toString: String = {
    val status = if(succeeded)
      "successful"
    else
      "failed"
    f"""|$status
        |(${successfulVerifications.size} succeeded /
        |${failedVerifications.size} failed)
        |bag verification in $formatDuration
        |"""
      .stripMargin
      .replaceAll("\n", " ")
  }
}

case class FailedVerification(
  digestFile: BagDigestFile,
  reason: Throwable
)
