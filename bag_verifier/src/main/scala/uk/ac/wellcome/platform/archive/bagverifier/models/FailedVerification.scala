package uk.ac.wellcome.platform.archive.bagverifier.models

import uk.ac.wellcome.platform.archive.common.models.bagit.BagDigestFile

case class FailedVerification(
  digestFile: BagDigestFile,
  reason: Throwable
)
