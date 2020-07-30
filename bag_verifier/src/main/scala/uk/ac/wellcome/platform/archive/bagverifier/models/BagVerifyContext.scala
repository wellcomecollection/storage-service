package uk.ac.wellcome.platform.archive.bagverifier.models

import uk.ac.wellcome.storage.{Location, Prefix}

sealed trait BagVerifyContext[BagPrefix <: Prefix[_ <: Location]] {
  val root: BagPrefix
}

case class StandaloneBagVerifyContext[BagPrefix <: Prefix[_ <: Location]](
  root: BagPrefix)
    extends BagVerifyContext[BagPrefix]

case class ReplicatedBagVerifyContext[BagPrefix <: Prefix[_ <: Location]](
  srcRoot: BagPrefix,
  replicaRoot: BagPrefix)
    extends BagVerifyContext[BagPrefix] {

  val root: BagPrefix = replicaRoot
}
