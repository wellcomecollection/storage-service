package weco.storage_service.bag_replicator.replicator.s3

import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.transfer.s3.S3TransferManager
import weco.storage_service.bag_replicator.replicator.Replicator
import weco.storage.providers.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import weco.storage.transfer.s3.S3PrefixTransfer

class S3Replicator(implicit s3Client: S3Client,
                   s3TransferManager: S3TransferManager)
    extends Replicator[
      S3ObjectLocation,
      S3ObjectLocation,
      S3ObjectLocationPrefix
    ] {

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

  override protected def buildDestinationFromParts(
    bucket: String,
    keyPrefix: String
  ): S3ObjectLocationPrefix =
    S3ObjectLocationPrefix(bucket = bucket, keyPrefix = keyPrefix)
}
