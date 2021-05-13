package uk.ac.wellcome.platform.storage.replica_aggregator.models

import uk.ac.wellcome.storage.azure.AzureBlobLocationPrefix
import uk.ac.wellcome.storage.s3.S3ObjectLocationPrefix
import uk.ac.wellcome.storage.{Location, Prefix, TypedStringScanamoOps}
import weco.json.TypedString

class ReplicaPath(val underlying: String) extends TypedString[ReplicaPath]

object ReplicaPath extends TypedStringScanamoOps[ReplicaPath] {
  override def apply(underlying: String): ReplicaPath =
    new ReplicaPath(underlying)

  def apply(prefix: Prefix[_ <: Location]): ReplicaPath =
    prefix match {
      case S3ObjectLocationPrefix(_, keyPrefix)   => ReplicaPath(keyPrefix)
      case AzureBlobLocationPrefix(_, namePrefix) => ReplicaPath(namePrefix)
    }
}
