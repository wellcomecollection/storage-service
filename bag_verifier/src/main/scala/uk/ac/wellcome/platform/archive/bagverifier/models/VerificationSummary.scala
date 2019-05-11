package uk.ac.wellcome.platform.archive.bagverifier.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.operation.models.Summary
import uk.ac.wellcome.platform.archive.common.verify.{VerificationFailure, VerificationSuccess}
import uk.ac.wellcome.storage.ObjectLocation

case class VerificationSummary(
                                rootLocation: ObjectLocation,
                                verificationSuccess: VerificationSuccess[Seq] = VerificationSuccess.empty,
                                verificationFailure: VerificationFailure[Seq] = VerificationFailure.empty,
                                startTime: Instant = Instant.now(),
                                endTime: Option[Instant] = None)
    extends Summary {

  def succeeded: Boolean = verificationFailure.locations.isEmpty
  def complete: VerificationSummary = this.copy(endTime = Some(Instant.now()))

  override def toString: String = {
    val status =
      if (succeeded)
        "successful"
      else
        "failed"
    f"""|bag=$rootLocation
        |status=$status
        |verified=${verificationSuccess.locations.size}
        |failed=${verificationFailure.locations.size}
        |durationSeconds=$durationSeconds
        |duration=$formatDuration""".stripMargin
      .replaceAll("\n", ", ")
  }
}
