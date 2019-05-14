package uk.ac.wellcome.platform.archive.common.storage.services

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.S3ObjectInputStream
import uk.ac.wellcome.platform.archive.common.storage.{Resolvable, Streamable}
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.Try

object S3StreamableInstances {

  implicit class ObjectLocationStreamable(location: ObjectLocation)(implicit s3Client: AmazonS3) {
    def toInputStream = {
      Try(
        s3Client.getObject(
          location.namespace,
          location.key
        )
      ).map(_.getObjectContent)
    }
  }

  implicit class ResolvableStreamable[T](t: T)(implicit s3Client: AmazonS3, resolver: Resolvable[T]) {
    def from(root: ObjectLocation) = {
      val streamable = new Streamable[T, S3ObjectInputStream] {
        override def stream(t: T): Try[S3ObjectInputStream] = {
          val resolved = resolver.resolve(root)(t)

          resolved.toInputStream
        }
      }

      streamable.stream(t)
    }
  }
}
