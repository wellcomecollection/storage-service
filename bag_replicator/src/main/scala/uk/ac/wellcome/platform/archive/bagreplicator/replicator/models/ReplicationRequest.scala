package uk.ac.wellcome.platform.archive.bagreplicator.replicator.models

import uk.ac.wellcome.storage.{ObjectLocationPrefix, S3ObjectLocationPrefix}

case class ReplicationRequest(
  srcPrefix: S3ObjectLocationPrefix,
  dstPrefix: ObjectLocationPrefix
)
