package uk.ac.wellcome.platform.archive.bagverifier.models

import uk.ac.wellcome.storage.{Location, Prefix}

sealed trait BagVerifyContext[BagLocation <: Location, BagPrefix <: Prefix[BagLocation]]{
  val root: BagPrefix
}
case class StandaloneBagVerifyContext[BagLocation <: Location, BagPrefix <: Prefix[BagLocation]](root: BagPrefix) extends BagVerifyContext[BagLocation, BagPrefix]
case class ReplicatedBagVerifyContext[BagLocation <: Location, BagPrefix <: Prefix[BagLocation]](root: BagPrefix, srcRoot: BagPrefix) extends BagVerifyContext[BagLocation, BagPrefix]
