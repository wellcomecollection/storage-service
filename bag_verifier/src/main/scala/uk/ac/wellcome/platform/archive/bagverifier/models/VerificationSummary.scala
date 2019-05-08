package uk.ac.wellcome.platform.archive.bagverifier.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.BagDigestFile
import uk.ac.wellcome.platform.archive.common.operation.models.Summary
import uk.ac.wellcome.storage.ObjectLocation

case class VerificationSummary(
  bagRootLocation: ObjectLocation,
  successfulVerifications: Seq[BagDigestFile] = List.empty,
  failedVerifications: Seq[FailedVerification] = List.empty,
  startTime: Instant = Instant.now(),
  endTime: Option[Instant] = None)
    extends Summary {
  def succeeded: Boolean = failedVerifications.isEmpty
  def complete: VerificationSummary = this.copy(endTime = Some(Instant.now()))

  override def toString: String = {
    val status =
      if (succeeded)
        "successful"
      else
        "failed"
    f"""|bag=$bagRootLocation
        |status=$status
        |verified=${successfulVerifications.size}
        |failed=${failedVerifications.size}
        |durationSeconds=$durationSeconds
        |duration=$formatDuration""".stripMargin
      .replaceAll("\n", ", ")
  }
}
