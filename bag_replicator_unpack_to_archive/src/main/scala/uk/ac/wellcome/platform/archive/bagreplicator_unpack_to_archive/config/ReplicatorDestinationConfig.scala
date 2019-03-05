package uk.ac.wellcome.platform.archive.bagreplicator_unpack_to_archive.config

// Specifies the S3 bucket and root path for objects copied by the replicator.
case class ReplicatorDestinationConfig(namespace: String,
                                       rootPath: Option[String])
