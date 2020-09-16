package uk.ac.wellcome.platform.archive.bagverifier.services.s3

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.bagverifier.fixity.FixityListChecker
import uk.ac.wellcome.platform.archive.bagverifier.fixity.s3.S3FixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.models.{
  BagVerifierError,
  BagVerifyContext,
  ReplicatedBagVerifyContext,
  StandaloneBagVerifyContext
}
import uk.ac.wellcome.platform.archive.bagverifier.services.{
  BagVerifier,
  ReplicatedBagVerifier
}
import uk.ac.wellcome.platform.archive.bagverifier.storage.Resolvable
import uk.ac.wellcome.platform.archive.bagverifier.storage.s3.S3Resolvable
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  Bag,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.bagit.services.BagReader
import uk.ac.wellcome.platform.archive.common.bagit.services.s3.S3BagReader
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.listing.Listing
import uk.ac.wellcome.storage.listing.s3.S3ObjectLocationListing
import uk.ac.wellcome.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.store.s3.S3StreamStore

trait S3BagVerifier[B <: BagVerifyContext[S3ObjectLocationPrefix]]
    extends BagVerifier[B, S3ObjectLocation, S3ObjectLocationPrefix] {

  override def getRelativePath(
    root: S3ObjectLocationPrefix,
    location: S3ObjectLocation
  ): String =
    location.key.replace(root.keyPrefix, "")
}

class S3StandaloneBagVerifier(
  val primaryBucket: String,
  val bagReader: BagReader[S3ObjectLocation, S3ObjectLocationPrefix],
  val listing: Listing[S3ObjectLocationPrefix, S3ObjectLocation],
  val resolvable: Resolvable[S3ObjectLocation],
  val fixityListChecker: FixityListChecker[
    S3ObjectLocation,
    S3ObjectLocationPrefix,
    Bag
  ]
) extends BagVerifier[
      StandaloneBagVerifyContext,
      S3ObjectLocation,
      S3ObjectLocationPrefix
    ]
    with S3BagVerifier[StandaloneBagVerifyContext] {
  override def verifyReplicatedBag(
    context: StandaloneBagVerifyContext,
    space: StorageSpace,
    externalIdentifier: ExternalIdentifier,
    bag: Bag
  ): Either[BagVerifierError, Unit] = Right(())

}

class S3ReplicatedBagVerifier(
  val primaryBucket: String,
  val bagReader: BagReader[S3ObjectLocation, S3ObjectLocationPrefix],
  val listing: Listing[S3ObjectLocationPrefix, S3ObjectLocation],
  val resolvable: Resolvable[S3ObjectLocation],
  val fixityListChecker: FixityListChecker[
    S3ObjectLocation,
    S3ObjectLocationPrefix,
    Bag
  ],
  val srcReader: StreamStore[S3ObjectLocation]
) extends ReplicatedBagVerifier[
      S3ObjectLocation,
      S3ObjectLocationPrefix
    ]
    with S3BagVerifier[
      ReplicatedBagVerifyContext[S3ObjectLocationPrefix]
    ]

object S3BagVerifier {
  def standalone(
    primaryBucket: String
  )(implicit s3Client: AmazonS3): S3StandaloneBagVerifier = {
    val bagReader = new S3BagReader()
    val listing = S3ObjectLocationListing()
    val resolvable = new S3Resolvable()
    implicit val fixityChecker = S3FixityChecker()
    val fixityListChecker =
      new FixityListChecker[S3ObjectLocation, S3ObjectLocationPrefix, Bag]()
    new S3StandaloneBagVerifier(
      primaryBucket,
      bagReader,
      listing,
      resolvable,
      fixityListChecker
    )
  }
  def replicated(
    primaryBucket: String
  )(implicit s3Client: AmazonS3): S3ReplicatedBagVerifier = {
    val bagReader = new S3BagReader()
    val listing = S3ObjectLocationListing()
    val resolvable = new S3Resolvable()
    implicit val fixityChecker = S3FixityChecker()
    val fixityListChecker =
      new FixityListChecker[S3ObjectLocation, S3ObjectLocationPrefix, Bag]()
    val streamStore = new S3StreamStore()
    new S3ReplicatedBagVerifier(
      primaryBucket,
      bagReader,
      listing,
      resolvable,
      fixityListChecker,
      srcReader = streamStore
    )
  }
}
