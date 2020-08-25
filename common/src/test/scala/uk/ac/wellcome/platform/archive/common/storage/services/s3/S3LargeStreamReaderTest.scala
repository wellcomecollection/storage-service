package uk.ac.wellcome.platform.archive.common.storage.services.s3

import com.amazonaws.services.s3.model.GetObjectRequest
import org.mockito.Mockito
import org.mockito.Matchers._
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.storage.services.{LargeStreamReader, LargeStreamReaderTestCases, RangedReader}
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.s3.S3ObjectLocation
import uk.ac.wellcome.storage.streaming.Codec.stringCodec

class S3LargeStreamReaderTest extends LargeStreamReaderTestCases[S3ObjectLocation, Bucket] with S3Fixtures {
  override def withNamespace[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  override def createIdentWith(bucket: Bucket): S3ObjectLocation =
    createS3ObjectLocationWith(bucket)

  override def writeString(location: S3ObjectLocation, contents: String): Unit =
    s3Client.putObject(location.bucket, location.key, contents)

  override def withLargeStreamReader[R](bufferSize: Long)(
    testWith: TestWith[LargeStreamReader[S3ObjectLocation], R]): R =
    testWith(new S3LargeStreamReader(bufferSize = bufferSize))

  override def withRangedReader[R](testWith: TestWith[RangedReader[S3ObjectLocation], R]): R =
    testWith(new S3RangedReader())

  override def withLargeStreamReader[R](bufferSize: Long, rangedReaderImpl: RangedReader[S3ObjectLocation])(testWith: TestWith[LargeStreamReader[S3ObjectLocation], R]): R =
    testWith(
      new S3LargeStreamReader(bufferSize = bufferSize) {
        override protected val rangedReader: RangedReader[S3ObjectLocation] = rangedReaderImpl
      }
    )

  it("makes multiple GetObject requests") {
    val bufferSize = 500

    withLocalS3Bucket { bucket =>
      val location = createS3ObjectLocationWith(bucket)
      putStream(location, inputStream = randomInputStream(length = bufferSize * 2))

      val spyClient = Mockito.spy(s3Client)
      val reader = new S3LargeStreamReader(bufferSize = bufferSize)(s3Client = spyClient)

      // Consume all the bytes from the stream, even if we don't look at them.
      val inputStream = reader.get(location).right.value.identifiedT
      stringCodec.fromStream(inputStream).right.value

      // One to get the size of the object, two to read the contents
      Mockito
        .verify(spyClient, Mockito.atLeast(3))
        .getObject(any[GetObjectRequest])
    }
  }
}
