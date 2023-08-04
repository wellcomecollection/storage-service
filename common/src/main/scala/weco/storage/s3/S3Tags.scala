package weco.storage.tags.s3

import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{
  GetObjectTaggingRequest,
  PutObjectTaggingRequest,
  Tag,
  Tagging
}
import weco.storage.s3.{S3Errors, S3ObjectLocation}
import weco.storage.store.RetryableReadable
import weco.storage.tags.Tags
import weco.storage._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class S3Tags(val maxRetries: Int = 3)(implicit s3Client: S3Client)
    extends Tags[S3ObjectLocation]
    with RetryableReadable[S3ObjectLocation, Map[String, String]] {

  override protected def retryableGetFunction(
    location: S3ObjectLocation): Map[String, String] = {
    val request = GetObjectTaggingRequest
      .builder()
      .bucket(location.bucket)
      .key(location.key)
      .build()

    val response = s3Client.getObjectTagging(request)

    response
      .tagSet()
      .asScala
      .map { tag: Tag =>
        tag.key() -> tag.value()
      }
      .toMap
  }

  override protected def buildGetError(throwable: Throwable): ReadError =
    S3Errors.readErrors(throwable)

  override protected def writeTags(
    location: S3ObjectLocation,
    tags: Map[String, String]
  ): Either[WriteError, Map[String, String]] = {
    import weco.storage.RetryOps._

    def inner: Either[WriteError, Map[String, String]] =
      writeTagsOnce(location, tags)

    inner.retry(maxRetries)
  }

  private def writeTagsOnce(
    location: S3ObjectLocation,
    tags: Map[String, String]): Either[WriteError, Map[String, String]] = {
    val tagSet = tags
      .map { case (k, v) => Tag.builder().key(k).value(v).build() }
      .toSeq
      .asJava

    val tagging =
      Tagging
        .builder()
        .tagSet(tagSet)
        .build()

    val request =
      PutObjectTaggingRequest
        .builder()
        .bucket(location.bucket)
        .key(location.key)
        .tagging(tagging)
        .build()

    Try {
      s3Client.putObjectTagging(request)
    } match {
      case Success(_)   => Right(tags)
      case Failure(err) => Left(S3Errors.writeErrors(err))
    }
  }
}
