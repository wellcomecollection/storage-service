package uk.ac.wellcome.platform.archive.bagverifier.models

import uk.ac.wellcome.platform.archive.common.models.bagit.BagDigestFile

case class BagVerification(
  wokeBaes: Seq[BagDigestFile],
  problematicFaves: Seq[FailedVerification]
) {
  def verificationSucceeded: Boolean = problematicFaves.isEmpty
}
