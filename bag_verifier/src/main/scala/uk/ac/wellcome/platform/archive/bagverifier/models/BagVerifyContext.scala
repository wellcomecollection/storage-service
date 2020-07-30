package uk.ac.wellcome.platform.archive.bagverifier.models

import uk.ac.wellcome.storage.{Location, Prefix}

sealed trait BagVerifyContext[BagPrefix <: Prefix[_ <: Location]] {
  val root: BagPrefix
}

case class StandaloneBagVerifyContext[BagPrefix <: Prefix[_ <: Location]](
  root: BagPrefix
) extends BagVerifyContext[BagPrefix]


case class ReplicatedBagVerifyContext[
  SrcBagPrefix <: Prefix[_ <: Location],
  ReplicaBagPrefix <: Prefix[_ <: Location]
](
  srcRoot: SrcBagPrefix,
  replicaRoot: ReplicaBagPrefix)
    extends BagVerifyContext[ReplicaBagPrefix] {

  val root: ReplicaBagPrefix = replicaRoot
}
