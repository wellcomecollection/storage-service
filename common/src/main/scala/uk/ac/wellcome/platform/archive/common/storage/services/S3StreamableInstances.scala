package uk.ac.wellcome.platform.archive.common.storage.services

import java.io.InputStream

import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.Try

object S3StreamableInstances {
  implicit class ObjectLocationStreamable(location: ObjectLocation)(
    implicit s3Client: AmazonS3)
    extends Logging {
    def toInputStream: Try[InputStream] = {
      debug(s"Converting $location to InputStream")

      val result = Try(
        s3Client.getObject(
          location.namespace,
          location.key
        )
      ).map(_.getObjectContent)

      debug(s"Got: $result")

      result
    }
  }
}