package uk.ac.wellcome.platform.archive.bagverifier.models

import uk.ac.wellcome.platform.archive.common.models.bagit.BagDigestFile

case class BagVerification(
  woke: Seq[BagDigestFile] = List.empty,
  problematicFaves: Seq[FailedVerification] = List.empty
) {
  def verificationSucceeded: Boolean = problematicFaves.isEmpty
}
