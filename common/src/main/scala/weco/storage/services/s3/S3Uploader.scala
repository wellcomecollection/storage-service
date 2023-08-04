package weco.storage.services.s3

import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.presigner.S3Presigner
import weco.storage._
import weco.storage.s3.S3ObjectLocation
import weco.storage.store.s3.S3StreamStore
import weco.storage.streaming.Codec.stringCodec
import weco.storage.streaming.InputStreamWithLength

import java.net.URL
import scala.concurrent.duration.Duration

/** This class allows you to upload a string to an S3 bucket, and get a
  * pre-signed URL for somebody to GET that string out of the bucket.
  */
class S3Uploader(implicit val s3Client: S3Client, s3Presigner: S3Presigner) {
  import S3ObjectExists._

  private val presignedUrls = new S3PresignedUrls()
  private val s3StreamStore: S3StreamStore = new S3StreamStore()

  // NOTE: checkExists will allow overwriting of existing content if set to false
  // overwriting existing content will change what previously generated URLs return
  def uploadAndGetURL(
    location: S3ObjectLocation,
    content: InputStreamWithLength,
    expiryLength: Duration,
    checkExists: Boolean
  ): Either[StorageError, URL] =
    for {
      exists <- location.exists

      _ <- if (!exists || !checkExists) {
        s3StreamStore.put(location)(content)
      } else {
        Right(Identified(location, content))
      }

      url <- presignedUrls.getPresignedGetURL(location, expiryLength)
    } yield url

  def uploadAndGetURL(
    location: S3ObjectLocation,
    content: String,
    expiryLength: Duration,
    checkExists: Boolean = false
  ): Either[StorageError, URL] =
    for {
      inputStream <- stringCodec.toStream(content)
      result <- uploadAndGetURL(
        location = location,
        content = inputStream,
        expiryLength = expiryLength,
        checkExists = checkExists
      )
    } yield result
}
