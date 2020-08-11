package uk.ac.wellcome.platform.archive.bagverifier.services

import uk.ac.wellcome.platform.archive.bagverifier.models.{
  BagVerifierError,
  ReplicatedBagVerifyContext
}
import uk.ac.wellcome.platform.archive.bagverifier.verify.steps.VerifySourceTagManifest
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  Bag,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.{Location, Prefix}

trait ReplicatedBagVerifier[
  ReplicaBagLocation <: Location,
  ReplicaBagPrefix <: Prefix[ReplicaBagLocation]
] extends BagVerifier[
      ReplicatedBagVerifyContext[ReplicaBagPrefix],
      ReplicaBagLocation,
      ReplicaBagPrefix
    ]
    with VerifySourceTagManifest[ReplicaBagLocation] {
  override def verifyReplicatedBag(
    root: ReplicatedBagVerifyContext[ReplicaBagPrefix],
    space: StorageSpace,
    externalIdentifier: ExternalIdentifier,
    bag: Bag
  ): Either[BagVerifierError, Unit] =
    verifySourceTagManifestIsTheSame(
      srcPrefix = root.srcRoot,
      replicaPrefix = root.replicaRoot
    )
}
