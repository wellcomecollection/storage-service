package uk.ac.wellcome.platform.archive.bagreplicator.replicator.models

import uk.ac.wellcome.storage.ObjectLocationPrefix

case class ReplicationRequest(
  srcPrefix: ObjectLocationPrefix,
  dstPrefix: ObjectLocationPrefix
)
