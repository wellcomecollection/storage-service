package uk.ac.wellcome.platform.archive.bagreplicator.replicator.s3

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.Replicator
import uk.ac.wellcome.storage.transfer.s3.S3PrefixTransfer

class S3Replicator(implicit s3Client: AmazonS3)
    extends Replicator {
  implicit val prefixTransfer: S3PrefixTransfer =
    S3PrefixTransfer()
}
