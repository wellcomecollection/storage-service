package uk.ac.wellcome.platform.storage.bags.api.services

import java.net.URL
import java.util

import com.amazonaws.HttpMethod
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.store.s3.S3StreamStore
import uk.ac.wellcome.storage.streaming.Codec._
import uk.ac.wellcome.storage.streaming.InputStreamWithLengthAndMetadata
import uk.ac.wellcome.storage.{ObjectLocation, ReadError, StorageError, StoreReadError}

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/** This class allows you to upload a string to an S3 bucket, and get a
  * pre-signed URL for somebody to GET that string out of the bucket.
  *
  * It's based on an example from the AWS SDK for Java docs:
  * https://docs.aws.amazon.com/AmazonS3/latest/dev/ShareObjectPreSignedURLJavaSDK.html
  */
class S3Uploader(implicit s3Client: AmazonS3) {
  private val s3StreamStore: S3StreamStore = new S3StreamStore()

  def uploadAndGetURL(
    location: ObjectLocation,
    content: InputStreamWithLengthAndMetadata,
    expiryLength: Duration
  ): Either[StorageError, URL] =
    for {
      _ <- s3StreamStore.put(location)(content)
      url <- getPresignedGetURL(location, expiryLength)
    } yield url

  def uploadAndGetURL(
    location: ObjectLocation,
    content: String,
    expiryLength: Duration
  ): Either[StorageError, URL] =
    for {
      inputStream <- stringCodec.toStream(content)
      result <- uploadAndGetURL(
        location = location,
        content = InputStreamWithLengthAndMetadata(inputStream, metadata = Map.empty),
        expiryLength = expiryLength
      )
    } yield result

  private def getPresignedGetURL(
    location: ObjectLocation,
    expiryLength: Duration): Either[ReadError, URL] = {

    // Based on an example from the AWS SDK for Java docs:
    // https://docs.aws.amazon.com/AmazonS3/latest/dev/ShareObjectPreSignedURLJavaSDK.html
    val currentTime = new util.Date()
    val currTimeMillis = currentTime.getTime
    val expTimeMillis = currTimeMillis + expiryLength.toMillis
    val expTime = new util.Date(expTimeMillis)

    val request = new GeneratePresignedUrlRequest(location.namespace, location.path)
      .withMethod(HttpMethod.GET)
      .withExpiration(expTime)

    Try { s3Client.generatePresignedUrl(request) } match {
      case Success(url) => Right(url)
      case Failure(err) => Left(StoreReadError(err))
    }
  }
}

class S3UploaderTest extends FunSpec with Matchers with S3Fixtures {
  it("works") {
    withLocalS3Bucket { bucket =>
      s3Client.putObject(
        bucket.name,
        "digitised/b1234/v1.json",
        "hello world"
      )

      // Set the presigned URL to expire after an hour.
      // Based on an example from the AWS SDK for Java docs:
      // https://docs.aws.amazon.com/AmazonS3/latest/dev/ShareObjectPreSignedURLJavaSDK.html
      val currentTime = new util.Date()
      val currTimeMillis = currentTime.getTime
      val expTimeMillis = currTimeMillis + 1000 * 60 * 60
      val expTime = new util.Date(expTimeMillis)

      // Generate the presigned URL
      val request = new GeneratePresignedUrlRequest(bucket.name, "digitised/b1234/v1.json")
        .withMethod(HttpMethod.GET)
        .withExpiration(expTime)

      val url = s3Client.generatePresignedUrl(request)

      println(url)
    }
  }
}
