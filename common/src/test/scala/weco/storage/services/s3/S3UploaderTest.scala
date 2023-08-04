package weco.storage.services.s3

import java.io.IOException
import java.net.URL
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.s3.model.{HeadObjectRequest, S3Exception}
import weco.storage._
import weco.storage.fixtures.S3Fixtures
import weco.storage.s3.S3ObjectLocation

import java.time.Instant
import scala.concurrent.duration._
import scala.io.Source

class S3UploaderTest extends AnyFunSpec with Matchers with S3Fixtures {
  val uploader = new S3Uploader()

  it("creates a pre-signed URL for an object") {
    val content = randomAlphanumeric()

    withLocalS3Bucket { bucket =>
      val url = uploader
        .uploadAndGetURL(
          location = createS3ObjectLocationWith(bucket),
          content = content,
          expiryLength = 5.minutes
        )
        .value

      getUrl(url) shouldBe content
    }
  }

  it("will not update an existing stored object if instructed so") {
    val content = randomAlphanumeric()

    withLocalS3Bucket { bucket =>
      val location = createS3ObjectLocationWith(bucket)

      val url = uploader
        .uploadAndGetURL(
          location = location,
          content = content,
          expiryLength = 5.minutes
        )
        .value

      val lastModified = getLastModified(location)

      getUrl(url) shouldBe content

      Thread.sleep(1500)

      val newUrl = uploader
        .uploadAndGetURL(
          location = location,
          content = content,
          expiryLength = 5.minutes,
          checkExists = true
        )
        .value

      val newLastModified = getLastModified(location)

      newUrl shouldNot equal(url)
      lastModified shouldBe newLastModified

      getUrl(newUrl) shouldBe content
    }
  }

  it("will update an existing stored object if instructed so") {
    val content = randomAlphanumeric()

    withLocalS3Bucket { bucket =>
      val location = createS3ObjectLocationWith(bucket)

      val url = uploader
        .uploadAndGetURL(
          location = location,
          content = content,
          expiryLength = 5.minutes
        )
        .value

      val lastModified = getLastModified(location)

      getUrl(url) shouldBe content

      Thread.sleep(1500)

      val newUrl = uploader
        .uploadAndGetURL(
          location = location,
          content = content,
          expiryLength = 5.minutes
        )
        .value

      val newLastModified = getLastModified(location)

      newUrl shouldNot equal(url)

      lastModified shouldNot equal(newLastModified)
      newLastModified should be >= lastModified

      getUrl(newUrl) shouldBe content
    }
  }

  it("expires the URL after the given duration") {
    val content = randomAlphanumeric()

    withLocalS3Bucket { bucket =>
      // Picking a good expiryLength here is tricky: too fast and the test becomes
      // flaky, but too slow and the test takes an age!
      //
      // 3 seconds seems to be reliable on my machine; if this test gets flaky,
      // consider bumping the expiryLength.
      val url = uploader
        .uploadAndGetURL(
          location = createS3ObjectLocationWith(bucket),
          content = content,
          expiryLength = 3.seconds
        )
        .value

      getUrl(url) shouldBe content

      Thread.sleep(3500)

      val thrown = intercept[IOException] {
        getUrl(url)
      }

      thrown.getMessage should startWith(
        "Server returned HTTP response code: 403"
      )
    }
  }

  it("fails if it cannot upload to the bucket") {
    val err = uploader
      .uploadAndGetURL(
        location = createS3ObjectLocation,
        content = randomAlphanumeric(),
        expiryLength = 5.minutes
      )
      .left
      .value

    err shouldBe a[StoreWriteError]
    err.e shouldBe a[S3Exception]
    err.e.getMessage should startWith("The specified bucket does not exist")
  }

  // TODO: Write a test for the case where generating the pre-signed URL fails.
  // (I assume this is possible, I'm just not sure how.)

  def getUrl(url: URL): String =
    Source.fromURL(url).mkString

  def getLastModified(location: S3ObjectLocation): Instant = {
    val request =
      HeadObjectRequest.builder()
        .bucket(location.bucket)
        .key(location.key)
        .build()

    s3Client.headObject(request).lastModified()
  }
}
