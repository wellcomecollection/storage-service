package uk.ac.wellcome.platform.archive.common.models

import java.util.UUID

import uk.ac.wellcome.platform.archive.common.models.bagit.BagLocation

case class ReplicationResult(
  archiveRequestId: UUID,
  srcBagLocation: BagLocation,
  dstBagLocation: BagLocation
)
