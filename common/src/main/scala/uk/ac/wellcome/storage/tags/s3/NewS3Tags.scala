package uk.ac.wellcome.storage.tags.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{ObjectTagging, SetObjectTaggingRequest, Tag}
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.tags.Tags

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class NewS3Tags(implicit s3Client: AmazonS3) extends Tags[S3ObjectLocation] {
  // The S3Tags doesn't expose writeTags() because it's a protected method, so
  // inline a complete copy of it here.
  // TODO: Upstream this into scala-storage.
  override protected def writeTags(location: S3ObjectLocation, tags: Map[String, String]): Either[WriteError, Map[String, String]] = {
    val tagSet = tags
      .map { case (k, v) => new Tag(k, v) }
      .toSeq
      .asJava

    Try {
      s3Client.setObjectTagging(
        new SetObjectTaggingRequest(
          location.bucket,
          location.key,
          new ObjectTagging(tagSet)
        )
      )
    } match {
      case Success(_)   => Right(tags)
      case Failure(err) => Left(StoreWriteError(err))
    }
  }

  private val underlying: S3Tags = new S3Tags()

  override def get(location: S3ObjectLocation): ReadEither =
    underlying
      .get(location.toObjectLocation)
      .map { case Identified(_, tags) => Identified(location, tags) }
}
