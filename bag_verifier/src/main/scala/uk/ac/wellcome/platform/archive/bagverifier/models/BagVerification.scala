package uk.ac.wellcome.platform.archive.bagverifier.models

import java.time.{Duration, Instant}

import uk.ac.wellcome.platform.archive.common.models.bagit.BagDigestFile

case class BagVerification(
  successfulVerifications: Seq[BagDigestFile] = List.empty,
  failedVerifications: Seq[FailedVerification] = List.empty,
  startTime: Instant = Instant.now(),
  endTime: Option[Instant] = None) {
  def duration: Option[Duration] = endTime.map(Duration.between(startTime, _))
  def verificationSucceeded: Boolean = failedVerifications.isEmpty
  def complete: BagVerification = this.copy(endTime = Some(Instant.now()))
}
