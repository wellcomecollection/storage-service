package uk.ac.wellcome.storage.store.s3

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.storage.{Identified, S3ObjectLocation}
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.streaming.InputStreamWithLength

class NewS3StreamStore(implicit val s3Client: AmazonS3) extends StreamStore[S3ObjectLocation] {
  private val underlying = new S3StreamStore()

  override def get(location: S3ObjectLocation): ReadEither =
    underlying
      .get(location.toObjectLocation)
      .map { case Identified(_, inputStream) => Identified(location, inputStream) }

  override def put(location: S3ObjectLocation)(inputStream: InputStreamWithLength): WriteEither =
    underlying
      .put(location.toObjectLocation)(inputStream)
      .map { case Identified(_, t) => Identified(location, t) }
}
