package uk.ac.wellcome.platform.storage.bags.api.services

import java.io.IOException
import java.net.URL

import com.amazonaws.services.s3.model.AmazonS3Exception
import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.fixtures.S3Fixtures

import scala.concurrent.duration._
import scala.io.Source

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
