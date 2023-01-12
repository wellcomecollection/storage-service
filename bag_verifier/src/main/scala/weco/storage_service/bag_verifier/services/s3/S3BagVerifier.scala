package weco.storage_service.bag_verifier.services.s3

import software.amazon.awssdk.services.s3.S3Client
import weco.storage_service.bag_verifier.fixity.FixityListChecker
import weco.storage_service.bag_verifier.fixity.s3.S3FixityChecker
import weco.storage_service.bag_verifier.models.{
  BagVerifierError,
  BagVerifyContext,
  ReplicatedBagVerifyContext,
  StandaloneBagVerifyContext
}
import weco.storage_service.bag_verifier.services.{
  BagVerifier,
  ReplicatedBagVerifier
}
import weco.storage_service.bag_verifier.storage.Resolvable
import weco.storage_service.bag_verifier.storage.s3.S3Resolvable
import weco.storage_service.bagit.models.{Bag, ExternalIdentifier}
import weco.storage_service.bagit.services.BagReader
import weco.storage_service.bagit.services.s3.S3BagReader
import weco.storage_service.storage.models.StorageSpace
import weco.storage.listing.Listing
import weco.storage.listing.s3.S3ObjectLocationListing
import weco.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import weco.storage.store.s3.S3StreamReader

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
  ],
  val streamReader: S3StreamReader
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
  val srcReader: S3StreamReader,
  val streamReader: S3StreamReader
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
  )(implicit s3Client: S3Client): S3StandaloneBagVerifier = {
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
      fixityListChecker,
      streamReader = new S3StreamReader()
    )
  }
  def replicated(
    primaryBucket: String
  )(implicit s3Client: S3Client): S3ReplicatedBagVerifier = {
    val bagReader = new S3BagReader()
    val listing = S3ObjectLocationListing()
    val resolvable = new S3Resolvable()
    implicit val fixityChecker = S3FixityChecker()
    val fixityListChecker =
      new FixityListChecker[S3ObjectLocation, S3ObjectLocationPrefix, Bag]()
    val srcReader = new S3StreamReader()
    new S3ReplicatedBagVerifier(
      primaryBucket,
      bagReader,
      listing,
      resolvable,
      fixityListChecker,
      srcReader = srcReader,
      streamReader = srcReader
    )
  }
}
