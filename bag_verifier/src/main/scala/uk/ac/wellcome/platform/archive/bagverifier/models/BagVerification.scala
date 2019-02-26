package uk.ac.wellcome.platform.archive.bagverifier.models

import uk.ac.wellcome.platform.archive.common.models.bagit.BagDigestFile

case class BagVerification(
  woke: Seq[BagDigestFile],
  problematicFaves: Seq[FailedVerification]
)
