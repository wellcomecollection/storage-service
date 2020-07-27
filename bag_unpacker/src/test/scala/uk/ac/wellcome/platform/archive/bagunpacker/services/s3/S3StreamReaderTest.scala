package uk.ac.wellcome.platform.archive.bagunpacker.services.s3

import org.mockito.{Mockito, Matchers => MockitoMatchers}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.streaming.Codec._

class S3StreamReaderTest
    extends AnyFunSpec
    with Matchers
    with S3Fixtures
    with StorageRandomThings {
  it("makes multiple GetObject requests") {
    withLocalS3Bucket { bucket =>
      val location = createS3ObjectLocationWith(bucket)
      putStream(location, inputStream = randomInputStream(length = 1000))

      val spyClient = Mockito.spy(s3Client)
      val reader = new S3StreamReader(bufferSize = 500)(s3Client = spyClient)

      // Consume all the bytes from the stream, even if we don't look at them.
      val inputStream = reader.get(location).right.value.identifiedT
      stringCodec.fromStream(inputStream).right.value

      // We expect to see at least three calls to getObject:
      //
      //    - One to get the size of the object
      //    - Two or more to read the object
      //
      Mockito
        .verify(spyClient, Mockito.atLeast(3))
        .getObject(MockitoMatchers.any())
    }
  }

  it("joins multiple GetObject streams correctly") {
    withLocalS3Bucket { bucket =>
      val message = randomAlphanumericWithLength(length = 1000)
      val location = createS3ObjectLocationWith(bucket)

      s3Client.putObject(location.bucket, location.key, message)

      val reader = new S3StreamReader(bufferSize = 500)

      val inputStream = reader.get(location).right.value.identifiedT
      stringCodec.fromStream(inputStream).right.value shouldBe message
    }
  }
}
