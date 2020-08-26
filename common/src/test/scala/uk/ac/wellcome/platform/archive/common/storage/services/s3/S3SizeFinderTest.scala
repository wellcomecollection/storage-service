package uk.ac.wellcome.platform.archive.common.storage.services.s3

import com.amazonaws.services.s3.model._
import org.mockito.Mockito._
import org.mockito.Matchers._
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.storage.services.{
  SizeFinder,
  SizeFinderTestCases
}
import uk.ac.wellcome.storage.DoesNotExistError
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.s3.S3ObjectLocation

class S3SizeFinderTest
    extends SizeFinderTestCases[S3ObjectLocation, Bucket]
    with S3Fixtures {

  override def withContext[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  override def withSizeFinder[R](
    testWith: TestWith[SizeFinder[S3ObjectLocation], R]
  )(implicit context: Bucket): R =
    testWith(new S3SizeFinder())

  override def createIdent(implicit bucket: Bucket): S3ObjectLocation =
    createS3ObjectLocationWith(bucket)

  override def createObject(location: S3ObjectLocation, contents: String)(
    implicit context: Bucket
  ): Unit =
    s3Client.putObject(location.bucket, location.key, contents)

  it("fails if the prefix is for a non-existent S3 bucket") {
    val sizeFinder = new S3SizeFinder()

    val result = sizeFinder.getSize(createS3ObjectLocation)

    result.left.value shouldBe a[DoesNotExistError]
    result.left.value.e shouldBe a[AmazonS3Exception]
  }

  it("finds the size of an object in Glacier") {
    // Note: CloudServer (the Docker image we use for mocking S3 in tests) doesn't
    // handle objects in Glacier correctly.
    //
    // Normally, calling GetObject on something in Glacier will return an error
    // (InvalidObjectState), but GetObjectMetadata works fine.  We use spy() here
    // because we can't rely on CloudServer to warn us itself.
    //
    // See https://github.com/scality/cloudserver/issues/2977
    withLocalS3Bucket { bucket =>
      val location = createS3ObjectLocationWith(bucket)

      val inputStream = randomInputStream()

      s3Client.putObject(
        new PutObjectRequest(location.bucket, location.key, inputStream, new ObjectMetadata())
          .withStorageClass(StorageClass.Glacier)
      )

      val spyClient = spy(s3Client)

      val sizeFinder = new S3SizeFinder()(spyClient)

      sizeFinder.getSize(location).right.value shouldBe inputStream.length

      verify(spyClient, never()).getObject(any[String], any[String])
      verify(spyClient, never()).getObject(any[GetObjectRequest])
    }
  }
}
