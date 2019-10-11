package uk.ac.wellcome.platform.storage.bags.api.services

import java.io.IOException
import java.net.URL
import java.util

import com.amazonaws.HttpMethod
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{AmazonS3Exception, GeneratePresignedUrlRequest}
import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.store.s3.S3StreamStore
import uk.ac.wellcome.storage.streaming.Codec._
import uk.ac.wellcome.storage.streaming.InputStreamWithLengthAndMetadata
import uk.ac.wellcome.storage._

import scala.concurrent.duration._
import scala.io.Source
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

class S3UploaderTest extends FunSpec with Matchers with S3Fixtures with EitherValues {
  val uploader = new S3Uploader()

  it("creates a pre-signed URL for an object") {
    val content = randomAlphanumeric

    withLocalS3Bucket { bucket =>
      val url = uploader.uploadAndGetURL(
        location = createObjectLocationWith(bucket),
        content = content,
        expiryLength = 5.minutes
      ).right.value

      getUrl(url) shouldBe content
    }
  }

  it("expires the URL after the given duration") {
    val content = randomAlphanumeric

    withLocalS3Bucket { bucket =>
      val url = uploader.uploadAndGetURL(
        location = createObjectLocationWith(bucket),
        content = content,
        expiryLength = 2.seconds
      ).right.value

      getUrl(url) shouldBe content

      Thread.sleep(3000)

      val thrown = intercept[IOException] {
        getUrl(url)
      }

      thrown.getMessage should startWith("Server returned HTTP response code: 403")
    }
  }

  it("fails if it cannot upload to the bucket") {
    val err = uploader.uploadAndGetURL(
      location = createObjectLocationWith(createBucket),
      content = randomAlphanumeric,
      expiryLength = 5.minutes
    ).left.value

    err shouldBe a[StoreWriteError]
    err.e shouldBe a[AmazonS3Exception]
    err.e.getMessage should startWith("The specified bucket does not exist")
  }

  // TODO: Write a test for the case where generating the pre-signed URL fails.
  // (I assume this is possible, I'm just not sure how.)

  def getUrl(url: URL): String =
    Source.fromURL(url).mkString
}
