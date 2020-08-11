package uk.ac.wellcome.platform.archive.bagverifier.services.s3

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.bagverifier.fixity.FixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.fixity.s3.S3FixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.models.{BagVerifierError, BagVerifyContext, ReplicatedBagVerifyContext, StandaloneBagVerifyContext}
import uk.ac.wellcome.platform.archive.bagverifier.services.{BagVerifier, ReplicatedBagVerifier}
import uk.ac.wellcome.platform.archive.bagverifier.storage.Resolvable
import uk.ac.wellcome.platform.archive.bagverifier.storage.s3.S3Resolvable
import uk.ac.wellcome.platform.archive.common.bagit.models.{Bag, ExternalIdentifier}
import uk.ac.wellcome.platform.archive.common.bagit.services.BagReader
import uk.ac.wellcome.platform.archive.common.bagit.services.s3.S3BagReader
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.listing.Listing
import uk.ac.wellcome.storage.listing.s3.S3ObjectLocationListing
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.store.s3.S3StreamStore
import uk.ac.wellcome.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}

trait S3BagVerifier[B <: BagVerifyContext[S3ObjectLocationPrefix]]
    extends BagVerifier[B, S3ObjectLocation, S3ObjectLocationPrefix] {

  implicit val s3Client: AmazonS3

  override implicit val bagReader
    : BagReader[S3ObjectLocation, S3ObjectLocationPrefix] =
    new S3BagReader()

  override implicit val resolvable: Resolvable[S3ObjectLocation] =
    new S3Resolvable()

  override implicit val fixityChecker
    : FixityChecker[S3ObjectLocation, S3ObjectLocationPrefix] =
    new S3FixityChecker()

  override implicit val listing
    : Listing[S3ObjectLocationPrefix, S3ObjectLocation] =
    S3ObjectLocationListing()

  override def getRelativePath(
    root: S3ObjectLocationPrefix,
    location: S3ObjectLocation
  ): String =
    location.key.replace(root.keyPrefix, "")
}

class S3StandaloneBagVerifier(val primaryBucketName: String)(
  implicit val s3Client: AmazonS3
) extends BagVerifier[
  StandaloneBagVerifyContext[S3ObjectLocationPrefix],
  S3ObjectLocation,
  S3ObjectLocationPrefix
]
    with S3BagVerifier[StandaloneBagVerifyContext[S3ObjectLocationPrefix]] {
  override def verifyReplicatedBag(root: StandaloneBagVerifyContext[S3ObjectLocationPrefix], space: StorageSpace, externalIdentifier: ExternalIdentifier, bag: Bag): Either[BagVerifierError, Unit] = Right(())
}

class S3ReplicatedBagVerifier(val primaryBucketName: String)(
  implicit val s3Client: AmazonS3
) extends ReplicatedBagVerifier[
      S3ObjectLocation,
      S3ObjectLocationPrefix
    ]
    with S3BagVerifier[
      ReplicatedBagVerifyContext[S3ObjectLocationPrefix]
    ] {

  override val replicaStreamStore: StreamStore[S3ObjectLocation] =
    new S3StreamStore()
}
