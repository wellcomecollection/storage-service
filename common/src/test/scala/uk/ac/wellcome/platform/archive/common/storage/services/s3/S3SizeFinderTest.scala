package uk.ac.wellcome.platform.archive.common.storage.services.s3

import com.amazonaws.services.s3.model.AmazonS3Exception
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.storage.services.{SizeFinder, SizeFinderTestCases}
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

    result.left.value.e shouldBe a[AmazonS3Exception]
  }
}
