package uk.ac.wellcome.storage.transfer.s3

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.storage.S3ObjectLocation
import uk.ac.wellcome.storage.transfer._

class NewS3Transfer(implicit s3Client: AmazonS3)
    extends NewTransfer[S3ObjectLocation, S3ObjectLocation] {

  override protected val underlying = new S3Transfer()
}
