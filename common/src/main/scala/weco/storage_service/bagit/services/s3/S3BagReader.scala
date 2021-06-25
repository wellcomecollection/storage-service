package weco.storage_service.bagit.services.s3

import com.amazonaws.services.s3.AmazonS3
import weco.storage_service.bagit.services.BagReader
import weco.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import weco.storage.store.s3.S3StreamStore

class S3BagReader()(implicit s3Client: AmazonS3)
    extends BagReader[S3ObjectLocation, S3ObjectLocationPrefix] {
  override implicit val readable: S3StreamStore =
    new S3StreamStore()
}
