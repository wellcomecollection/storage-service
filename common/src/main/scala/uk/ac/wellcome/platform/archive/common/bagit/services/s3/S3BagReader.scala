package uk.ac.wellcome.platform.archive.common.bagit.services.s3

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.common.bagit.services.BagReader
import uk.ac.wellcome.storage.store.s3.S3StreamStore
import uk.ac.wellcome.storage.streaming.InputStreamWithLengthAndMetadata

class S3BagReader()(implicit s3Client: AmazonS3)
    extends BagReader[InputStreamWithLengthAndMetadata] {
  override implicit val streamStore: S3StreamStore =
    new S3StreamStore()
}
