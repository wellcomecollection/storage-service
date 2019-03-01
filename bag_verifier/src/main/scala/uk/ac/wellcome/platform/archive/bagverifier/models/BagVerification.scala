package uk.ac.wellcome.platform.archive.bagverifier.models

import java.time.Duration

import uk.ac.wellcome.platform.archive.common.models.bagit.BagDigestFile

case class BagVerification(successfulVerifications: Seq[BagDigestFile] = List.empty,
                           failedVerifications: Seq[FailedVerification] = List.empty,
                           duration: Duration) {
  def verificationSucceeded: Boolean = failedVerifications.isEmpty
}
