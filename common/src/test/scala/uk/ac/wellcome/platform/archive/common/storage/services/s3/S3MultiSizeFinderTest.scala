package uk.ac.wellcome.platform.archive.common.storage.services.s3

import com.amazonaws.services.s3.model.{GetObjectMetadataRequest, ListObjectsV2Request}
import org.mockito.Matchers._
import org.mockito.Mockito
import org.mockito.Mockito._
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.platform.archive.common.storage.services.{SizeFinder, SizeFinderTestCases}
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.s3.S3ObjectLocation

class S3MultiSizeFinderTest extends SizeFinderTestCases[S3ObjectLocation, Bucket] with S3Fixtures with StorageRandomThings {
  override def withContext[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  override def withSizeFinder[R](
    testWith: TestWith[SizeFinder[S3ObjectLocation], R]
  )(implicit context: Bucket): R =
    testWith(new S3MultiSizeFinder())

  override def createIdent(implicit bucket: Bucket): S3ObjectLocation =
    createS3ObjectLocationWith(bucket)

  override def createObject(location: S3ObjectLocation, contents: String)(
    implicit context: Bucket
  ): Unit =
    s3Client.putObject(location.bucket, location.key, contents)

  it("finds thousands of objects with only a handful of API calls") {
    val spyClient = spy(s3Client)

    val finder = new S3MultiSizeFinder()(spyClient)

    withLocalS3Bucket { bucket =>
      // More objects makes the test go longer; the key is to have more objects
      // than get returned in a single ListObjectsV2 API call (1000).
      val locations = (1 to 1100).map { size =>
        val key = s"file-$size.txt"

        s3Client.putObject(bucket.name, key, randomAlphanumericWithLength(size))

        size -> S3ObjectLocation(bucket.name, key)
      }

      locations.foreach { case (s, loc) =>
        finder.getSize(loc).right.value shouldBe s
      }

      // We don't care about the exact number of calls; the important thing is
      // that we're not making 1100 of them!
      verify(spyClient, Mockito.atMost(5)).listObjectsV2(any[ListObjectsV2Request])
      verify(spyClient, Mockito.never()).getObjectMetadata(any[String], any[String])
      verify(spyClient, Mockito.never()).getObjectMetadata(any[GetObjectMetadataRequest])
    }
  }

  it("gets the size of an object missed by ListObjectsV2") {
    val finder = new S3MultiSizeFinder()

    withLocalS3Bucket { bucket =>
      // This is because of an S3 ListObjectsV2 API limitation -- you can list
      // objects *after* a given key, but not *starting* at a given key.
      //
      // This means our ListObjects request might miss the object we're interested
      // in if the key layout causes a lookup miss.
      (1 to 1000).foreach { i =>
        s3Client.putObject(bucket.name, s"file-a$i", randomAlphanumeric)
      }

      val size = randomInt(from = 10, to = 100)
      s3Client.putObject(bucket.name, "file-b", randomAlphanumericWithLength(size))

      val location = S3ObjectLocation(bucket.name, key = "file-b")

      finder.getSize(location).right.value shouldBe size
    }
  }
}
