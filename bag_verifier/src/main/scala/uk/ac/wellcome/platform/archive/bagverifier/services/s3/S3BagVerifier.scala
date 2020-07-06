package uk.ac.wellcome.platform.archive.bagverifier.services.s3

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.bagverifier.fixity.FixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.fixity.s3.S3FixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.services.{BagVerifier, ReplicatedBagVerifier, UnpackedBagVerifier}
import uk.ac.wellcome.platform.archive.bagverifier.storage.Resolvable
import uk.ac.wellcome.platform.archive.bagverifier.storage.s3.S3Resolvable
import uk.ac.wellcome.platform.archive.common.bagit.services.BagReader
import uk.ac.wellcome.platform.archive.common.bagit.services.s3.S3BagReader
import uk.ac.wellcome.storage.listing.Listing
import uk.ac.wellcome.storage.listing.s3.NewS3ObjectLocationListing
import uk.ac.wellcome.storage.{S3ObjectLocation, S3ObjectLocationPrefix}

trait S3BagVerifier
    extends BagVerifier[S3ObjectLocation, S3ObjectLocationPrefix] {
  val primaryBucket: String

  implicit val s3Client: AmazonS3

  override val namespace: String = primaryBucket

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

class S3UnpackedBagVerifier(val primaryBucket: String)(implicit val s3Client: AmazonS3)
  extends S3BagVerifier
    with UnpackedBagVerifier[S3ObjectLocation, S3ObjectLocationPrefix]

class S3ReplicatedBagVerifier(val primaryBucket: String)(implicit val s3Client: AmazonS3)
  extends S3BagVerifier
    with ReplicatedBagVerifier[S3ObjectLocation, S3ObjectLocationPrefix]
