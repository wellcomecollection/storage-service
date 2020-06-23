package uk.ac.wellcome.storage.store.s3

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.storage.S3ObjectLocation
import uk.ac.wellcome.storage.store.TypedStore
import uk.ac.wellcome.storage.streaming.Codec

class NewS3TypedStore[T](
  implicit val codec: Codec[T],
  val streamStore: NewS3StreamStore
) extends TypedStore[S3ObjectLocation, T]

object NewS3TypedStore {
  def apply[T](
    implicit codec: Codec[T],
    s3Client: AmazonS3
  ): NewS3TypedStore[T] = {
    implicit val streamStore: NewS3StreamStore = new NewS3StreamStore()

    new NewS3TypedStore[T]()
  }
}
