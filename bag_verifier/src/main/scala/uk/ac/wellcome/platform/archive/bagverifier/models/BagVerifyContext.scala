package uk.ac.wellcome.platform.archive.bagverifier.models

import uk.ac.wellcome.storage.s3.S3ObjectLocationPrefix
import uk.ac.wellcome.storage.{Location, Prefix}

sealed trait BagVerifyContext[BagPrefix <: Prefix[_ <: Location]] {
  val root: BagPrefix
}

case class StandaloneBagVerifyContext(
  root: S3ObjectLocationPrefix
) extends BagVerifyContext[S3ObjectLocationPrefix]

case class ReplicatedBagVerifyContext[
  ReplicaBagPrefix <: Prefix[_ <: Location]
](srcRoot: S3ObjectLocationPrefix, replicaRoot: ReplicaBagPrefix)
    extends BagVerifyContext[ReplicaBagPrefix] {

  val root: ReplicaBagPrefix = replicaRoot
}
