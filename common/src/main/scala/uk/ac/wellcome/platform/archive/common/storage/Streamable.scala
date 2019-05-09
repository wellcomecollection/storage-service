package uk.ac.wellcome.platform.archive.common.storage

import java.io.InputStream

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.storage.ObjectLocation

import scala.concurrent.{ExecutionContext, Future}

trait Streamable[T] {
  def apply(t: T)(implicit s3Client: AmazonS3,
                  ec: ExecutionContext): Future[InputStream]
}

object Streamable {

  implicit class StreamableFuture[T](t: T) {
    def toInputStream(implicit toInputStream: Streamable[T],
                      s3Client: AmazonS3,
                      ec: ExecutionContext): Future[InputStream] = {
      toInputStream.apply(t)
    }
  }

  implicit object Streamable extends Streamable[ObjectLocation] {

    def apply(objectLocation: ObjectLocation)(
      implicit s3Client: AmazonS3,
      ec: ExecutionContext): Future[InputStream] =
      Future(
        s3Client.getObject(objectLocation.namespace, objectLocation.key)
      ).map(
        response => response.getObjectContent
      )
  }
}
