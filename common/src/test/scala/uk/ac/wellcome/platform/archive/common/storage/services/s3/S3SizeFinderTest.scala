package uk.ac.wellcome.platform.archive.common.storage.services.s3

import com.amazonaws.services.s3.model.AmazonS3Exception
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.storage.services.{
  SizeFinder, SizeFinderTestCases, S3SizeFinder}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket

class S3SizeFinderTest
    extends SizeFinderTestCases[ObjectLocation, Bucket]
    with S3Fixtures {

  override def withContext[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  override def withSizeFinder[R](testWith: TestWith[SizeFinder[ObjectLocation], R])(
      implicit context: Bucket): R =
    testWith(new S3SizeFinder())

  override def createIdent(implicit bucket: Bucket): ObjectLocation =
    createObjectLocationWith(bucket)

  override def createObject(location: ObjectLocation, contents: String)(implicit context: Bucket): Unit =
    s3Client.putObject(location.namespace, location.path, contents)

  it("fails if the prefix is for a non-existent S3 bucket") {
    val sizeFinder = new S3SizeFinder()

    val result = sizeFinder.getSize(createObjectLocation)

    result.left.value.e shouldBe a[AmazonS3Exception]
  }
}
