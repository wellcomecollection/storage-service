package weco.storage_service.bagit.services.s3

import software.amazon.awssdk.services.s3.S3Client
import weco.storage.providers.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import weco.storage.store.s3.S3StreamReader
import weco.storage_service.bagit.services.BagReader

class S3BagReader()(implicit s3Client: S3Client)
    extends BagReader[S3ObjectLocation, S3ObjectLocationPrefix] {
  override implicit val readable: S3StreamReader =
    new S3StreamReader()
}
