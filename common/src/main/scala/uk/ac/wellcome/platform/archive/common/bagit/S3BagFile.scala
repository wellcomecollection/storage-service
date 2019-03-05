package uk.ac.wellcome.platform.archive.common.bagit

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.iterable.S3Objects
import uk.ac.wellcome.storage.ObjectLocation

import scala.collection.JavaConverters._
import scala.util.Try

class S3BagFile(s3Client: AmazonS3, batchSize: Int = 1000) {
  def locateBagInfo(objectLocation: ObjectLocation): Try[String] = {
    val keys =
      S3Objects
        .withPrefix(
          s3Client,
          objectLocation.namespace,
          objectLocation.key
        )
        .withBatchSize(batchSize)
        .iterator()
        .asScala
        .map { _.getKey }

    BagInfoLocator.locateBagInfo(keys)
  }
}
