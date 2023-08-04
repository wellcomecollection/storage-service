package weco.storage.tags.s3

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.stubbing.OngoingStubbing
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._
import weco.fixtures.TestWith
import weco.storage.UpdateWriteError
import weco.storage.fixtures.S3Fixtures
import weco.storage.fixtures.S3Fixtures.Bucket
import weco.storage.s3.S3ObjectLocation
import weco.storage.tags.{Tags, TagsTestCases}

import scala.collection.JavaConverters._

class S3TagsTest
    extends AnyFunSpec
    with Matchers
    with TagsTestCases[S3ObjectLocation, Bucket]
    with S3Fixtures
    with MockitoSugar {
  // We can associate with at most 10 tags on an object; see
  // https://docs.aws.amazon.com/AmazonS3/latest/dev/object-tagging.html
  override val maxTags: Int = 10

  override def withTags[R](
    initialTags: Map[S3ObjectLocation, Map[String, String]])(
    testWith: TestWith[Tags[S3ObjectLocation], R]): R = {
    initialTags
      .foreach {
        case (location, tags) =>
          putStream(location)

          val tagSet = tags
            .map { case (k, v) => Tag.builder().key(k).value(v).build() }
            .toSeq
            .asJava

          val tagging = Tagging.builder()
            .tagSet(tagSet)
            .build()

          val request = PutObjectTaggingRequest.builder()
            .bucket(location.bucket)
            .key(location.key)
            .tagging(tagging)
            .build()

          s3Client.putObjectTagging(request)
      }

    testWith(new S3Tags())
  }

  override def createIdent(bucket: Bucket): S3ObjectLocation =
    createS3ObjectLocationWith(bucket)

  override def withContext[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  val s3Tags = new S3Tags()

  describe("handles S3-specific errors") {
    // We can associate with at most 10 tags on an object; see
    // https://docs.aws.amazon.com/AmazonS3/latest/dev/object-tagging.html
    it("if you send more than 10 tags") {
      val newTags = (1 to 11).map { i =>
        s"key-$i" -> s"value-$i"
      }.toMap

      withLocalS3Bucket { bucket =>
        val location = createS3ObjectLocationWith(bucket)
        putStream(location)

        val result =
          s3Tags
            .update(location) { existingTags: Map[String, String] =>
              Right(existingTags ++ newTags)
            }

        assertIsS3Exception(result) {
          _ should startWith("Object tags cannot be greater than 10")
        }
      }
    }

    it("if the tag name is empty") {
      withLocalS3Bucket { bucket =>
        val location = createS3ObjectLocationWith(bucket)
        putStream(location)

        val result =
          s3Tags
            .update(location) { existingTags: Map[String, String] =>
              Right(existingTags ++ Map("" -> "value"))
            }

        assertIsS3Exception(result) {
          _ should startWith("The TagKey you have provided is invalid")
        }
      }
    }

    it("if the tag name is too long") {
      // A tag key can be at most 128 characters long.
      // https://docs.aws.amazon.com/AmazonS3/latest/dev/object-tagging.html
      withLocalS3Bucket { bucket =>
        val location = createS3ObjectLocationWith(bucket)
        putStream(location)

        val result =
          s3Tags
            .update(location) { existingTags: Map[String, String] =>
              Right(
                existingTags ++ Map(
                  randomAlphanumeric(length = 129) -> "value"))
            }

        assertIsS3Exception(result) {
          _ should startWith("The TagKey you have provided is invalid")
        }
      }
    }

    it("if the tag value is too long") {
      // A tag value can be at most 256 characters long.
      // https://docs.aws.amazon.com/AmazonS3/latest/dev/object-tagging.html
      withLocalS3Bucket { bucket =>
        val location = createS3ObjectLocationWith(bucket)
        putStream(location)

        val result: s3Tags.UpdateEither =
          s3Tags
            .update(location) { existingTags: Map[String, String] =>
              Right(
                existingTags ++ Map("key" -> randomAlphanumeric(length = 257)))
            }

        assertIsS3Exception(result) {
          _ should startWith("The TagValue you have provided is invalid")
        }
      }
    }
  }

  val s3ServerException = S3Exception.builder()
    .message("We encountered an internal error. Please try again.")
    .statusCode(500)
    .build()

  describe("retries flaky errors from the SetObjectTagging API") {
    it("doesn't retry a persistent error") {
      val mockClient = mock[S3Client]

      withLocalS3Bucket { bucket =>
        val location = createS3ObjectLocationWith(bucket)
        putStream(location)

        createGetObjectTaggingMock(mockClient, location = location)

        // This simulates the case where the object is deleted between the
        // GET call to find the current tags, and the SET call to update them.
        // That would be quite unusual and worth further investigation.
        when(mockClient.putObjectTagging(any[PutObjectTaggingRequest]))
          .thenThrow(
            S3Exception.builder()
              .message("The specified key does not exist")
              .statusCode(404)
              .build()
          )

        val tags = new S3Tags()(s3Client = mockClient)
        tags.update(location) { _ =>
          Right(Map("colour" -> "red"))
        } shouldBe a[Left[_, _]]

        verify(mockClient, times(1))
          .putObjectTagging(any[PutObjectTaggingRequest])
      }
    }

    it("it retries a single error") {
      val mockClient = mock[S3Client]

      withLocalS3Bucket { bucket =>
        val location = createS3ObjectLocationWith(bucket)
        putStream(location)

        createGetObjectTaggingMock(mockClient, location = location)

        when(mockClient.putObjectTagging(any[PutObjectTaggingRequest]))
          .thenThrow(s3ServerException)
          .thenReturn({
            val tagging =
              Tagging.builder()
                .tagSet(Seq(Tag.builder().key("colour").value("red").build()).asJava)
                .build()

            val request =
              PutObjectTaggingRequest.builder()
                .bucket(location.bucket)
                .key(location.key)
                .tagging(tagging)
                .build()

            s3Client.putObjectTagging(request)
          })

        val tags = new S3Tags()(s3Client = mockClient)
        tags.update(location) { _ =>
          Right(Map("colour" -> "red"))
        } shouldBe a[Right[_, _]]

        verify(mockClient, times(2))
          .putObjectTagging(any[PutObjectTaggingRequest])

        // Use a fresh instance of S3Tags so we use the unmocked client,
        // and not one with a mocked return value for getObjectTagging.
        new S3Tags().get(location).value.identifiedT shouldBe Map(
          "colour" -> "red")
      }
    }

    it("gives up if there are too many flaky errors") {
      val mockClient = mock[S3Client]

      withLocalS3Bucket { bucket =>
        val location = createS3ObjectLocationWith(bucket)
        putStream(location)

        createGetObjectTaggingMock(mockClient, location = location)

        when(mockClient.putObjectTagging(any[PutObjectTaggingRequest]))
          .thenThrow(s3ServerException)
          .thenThrow(s3ServerException)
          .thenThrow(s3ServerException)
          .thenThrow(s3ServerException)

        val tags = new S3Tags(maxRetries = 4)(s3Client = mockClient)
        tags.update(location) { _ =>
          Right(Map("colour" -> "blue"))
        } shouldBe a[Left[_, _]]

        verify(mockClient, times(4))
          .putObjectTagging(any[PutObjectTaggingRequest])
      }
    }

    def createGetObjectTaggingMock(
      mockClient: S3Client,
      location: S3ObjectLocation): OngoingStubbing[GetObjectTaggingResponse] =
      when(mockClient.getObjectTagging(any[GetObjectTaggingRequest]))
        .thenReturn(
          s3Client
            .getObjectTagging(
              GetObjectTaggingRequest.builder()
                .bucket(location.bucket)
                .key(location.key)
                .build()
            )
        )
  }

  private def assertIsS3Exception(result: s3Tags.UpdateEither)(
    assert: String => Assertion): Assertion = {
    val err = result.left.value

    err shouldBe a[UpdateWriteError]
    err.e shouldBe a[S3Exception]
    assert(err.e.getMessage)
  }
}
