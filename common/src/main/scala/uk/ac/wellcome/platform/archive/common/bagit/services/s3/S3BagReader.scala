package uk.ac.wellcome.platform.archive.common.bagit.services.s3

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.common.bagit.services.BagReader
import uk.ac.wellcome.storage.store.s3.NewS3StreamStore
import uk.ac.wellcome.storage.{S3ObjectLocation, S3ObjectLocationPrefix}

class S3BagReader()(implicit s3Client: AmazonS3)
    extends BagReader[S3ObjectLocation, S3ObjectLocationPrefix] {
  override implicit val readable: NewS3StreamStore =
    new NewS3StreamStore()
}
