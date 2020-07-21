package uk.ac.wellcome.platform.archive.bagreplicator.replicator.s3

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.Replicator
import uk.ac.wellcome.storage.listing.s3.{
  NewS3ObjectLocationListing,
  S3ObjectSummaryListing
}
import uk.ac.wellcome.storage.transfer.s3.S3PrefixTransfer

class S3Replicator(implicit s3Client: AmazonS3) extends Replicator {

  // We write new objects as Standard, and then rely on bucket management policies
  // to lifecycle objects to Standard-IA or Glacier Deep Archive as appropriate.
  //
  // We don't write straight as Standard-IA (although we used to) because that
  // has a minimum 128KB size for objects.  If you store <128KB, it rounds up and
  // charges you as if you were storing 128KB.
  //
  // Things like the bag-info.txt and tag manifest are tiny, and it's more expensive
  // to store them as Standard-IA than Standard.
  //
  implicit val prefixTransfer: S3PrefixTransfer = S3PrefixTransfer()

  implicit val s3ObjectSummaryListing: S3ObjectSummaryListing =
    new S3ObjectSummaryListing()

  override implicit val prefixListing: NewS3ObjectLocationListing =
    new NewS3ObjectLocationListing()
}
