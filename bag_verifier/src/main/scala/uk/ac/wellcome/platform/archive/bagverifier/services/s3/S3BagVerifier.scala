package uk.ac.wellcome.platform.archive.bagverifier.services.s3

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.bagverifier.fixity.FixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.fixity.s3.S3FixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.models.{BagVerifyContext, ReplicatedBagVerifyContext, StandaloneBagVerifyContext}
import uk.ac.wellcome.platform.archive.bagverifier.services.{BagVerifier, ReplicatedBagVerifier, StandaloneBagVerifier}
import uk.ac.wellcome.platform.archive.bagverifier.storage.Resolvable
import uk.ac.wellcome.platform.archive.bagverifier.storage.s3.S3Resolvable
import uk.ac.wellcome.platform.archive.common.bagit.services.BagReader
import uk.ac.wellcome.platform.archive.common.bagit.services.s3.S3BagReader
import uk.ac.wellcome.storage.listing.Listing
import uk.ac.wellcome.storage.listing.s3.NewS3ObjectLocationListing
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.store.s3.NewS3StreamStore
import uk.ac.wellcome.storage.{S3ObjectLocation, S3ObjectLocationPrefix}

trait S3BagVerifier[B <: BagVerifyContext[S3ObjectLocation, S3ObjectLocationPrefix]] extends BagVerifier[B,S3ObjectLocation, S3ObjectLocationPrefix] {


  implicit val s3Client: AmazonS3

  override def createPrefix(
                             bucket: String,
                             keyPrefix: String
                           ): S3ObjectLocationPrefix =
    S3ObjectLocationPrefix(bucket = namespace, keyPrefix = keyPrefix)

  override implicit val bagReader
  : BagReader[S3ObjectLocation, S3ObjectLocationPrefix] =
    new S3BagReader()

  override implicit val resolvable: Resolvable[S3ObjectLocation] =
    new S3Resolvable()

  override implicit val fixityChecker: FixityChecker[S3ObjectLocation] =
    new S3FixityChecker()

  override implicit val listing
  : Listing[S3ObjectLocationPrefix, S3ObjectLocation] =
    new NewS3ObjectLocationListing()
}


class S3StandaloneBagVerifier(primaryBucket: String)(implicit val s3Client: AmazonS3)
    extends StandaloneBagVerifier[S3ObjectLocation, S3ObjectLocationPrefix] with S3BagVerifier[StandaloneBagVerifyContext[S3ObjectLocation, S3ObjectLocationPrefix]] {
  override val namespace: String = primaryBucket
}

class S3ReplicatedBagVerifier(primaryBucket: String)(implicit val s3Client: AmazonS3) extends ReplicatedBagVerifier [S3ObjectLocation, S3ObjectLocationPrefix] with S3BagVerifier[ReplicatedBagVerifyContext[S3ObjectLocation, S3ObjectLocationPrefix]]{
  override val namespace: String = primaryBucket
  override val streamStore: StreamStore[S3ObjectLocation] = new NewS3StreamStore()
}
