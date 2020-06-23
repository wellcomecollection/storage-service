package uk.ac.wellcome.platform.archive.common.storage.services.s3

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.storage.services.{NewSizeFinder, NewSizeFinderTestCases}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket

class NewS3SizeFinderTest
    extends NewSizeFinderTestCases[ObjectLocation, Bucket]
    with S3Fixtures {

  override def withContext[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  override def withSizeFinder[R](testWith: TestWith[NewSizeFinder[ObjectLocation], R])(
      implicit context: Bucket): R =
    testWith(
      new NewS3SizeFinder()
    )

  override def createIdent(implicit bucket: Bucket): ObjectLocation =
    createObjectLocationWith(bucket)

  override def createObject(location: ObjectLocation, contents: String)(implicit context: Bucket): Unit =
    s3Client.putObject(location.namespace, location.path, contents)
}
