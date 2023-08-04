package weco.storage.services.s3

import software.amazon.awssdk.services.s3.model.S3Exception
import weco.fixtures.TestWith
import weco.storage.DoesNotExistError
import weco.storage.fixtures.S3Fixtures
import weco.storage.fixtures.S3Fixtures.Bucket
import weco.storage.generators.StreamGenerators
import weco.storage.s3.S3ObjectLocation
import weco.storage.services.{SizeFinder, SizeFinderTestCases}

class S3SizeFinderTest
    extends SizeFinderTestCases[S3ObjectLocation, Bucket]
    with S3Fixtures
    with StreamGenerators {

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
    putString(location, contents)

  it("fails if the prefix is for a non-existent S3 bucket") {
    val sizeFinder = new S3SizeFinder()

    val result = sizeFinder.getSize(createS3ObjectLocation)

    result.left.value shouldBe a[DoesNotExistError]
    result.left.value.e shouldBe a[S3Exception]
  }
}
