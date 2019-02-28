package uk.ac.wellcome.platform.archive.bagverifier.models

import java.time.Duration

import uk.ac.wellcome.platform.archive.common.models.bagit.BagDigestFile

case class BagVerification(successfulVerifications: Seq[BagDigestFile],
                           failedVerifications: Seq[FailedVerification],
                           duration: Duration) {
  def verificationSucceeded: Boolean = failedVerifications.isEmpty
}
