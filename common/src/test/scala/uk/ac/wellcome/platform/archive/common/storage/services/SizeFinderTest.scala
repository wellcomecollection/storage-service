package uk.ac.wellcome.platform.archive.common.storage.services

import com.amazonaws.services.s3.model.AmazonS3Exception
import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.storage.{DoesNotExistError, ObjectLocation}
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.s3.S3ObjectLocation
import uk.ac.wellcome.storage.store.memory.MemoryStreamStore
import uk.ac.wellcome.storage.streaming.Codec._

trait SizeFinderTestCases[Ident, Context]
    extends AnyFunSpec
    with Matchers
    with EitherValues {
  def withContext[R](testWith: TestWith[Context, R]): R

  def withSizeFinder[R](testWith: TestWith[SizeFinder[Ident], R])(
    implicit context: Context
  ): R

  def createIdent(implicit context: Context): Ident

  def createObject(ident: Ident, contents: String)(
    implicit context: Context
  ): Unit

  describe("it behaves as a size finder") {
    it("finds the sizes of objects in a prefix") {
      withContext { implicit context =>
        val ident = createIdent
        createObject(ident, "the quick brown fox")

        val result = withSizeFinder {
          _.getSize(ident)
        }

        result.right.value shouldBe 19L
      }
    }

    it("returns a DoesNotExistError if the object doesn't exist") {
      withContext { implicit context =>
        val location = createIdent

        val result = withSizeFinder {
          _.getSize(location)
        }

        result.left.value shouldBe a[DoesNotExistError]
      }
    }
  }
}

class MemorySizeFinderTest
    extends SizeFinderTestCases[ObjectLocation, MemoryStreamStore[ObjectLocation]]
    with StorageRandomThings {
  override def withContext[R](
    testWith: TestWith[MemoryStreamStore[ObjectLocation], R]
  ): R =
    testWith(MemoryStreamStore[ObjectLocation]())

  override def withSizeFinder[R](
    testWith: TestWith[SizeFinder[ObjectLocation], R]
  )(implicit streamStore: MemoryStreamStore[ObjectLocation]): R =
    testWith(
      new MemorySizeFinder(streamStore.memoryStore)
    )

  override def createIdent(
    implicit streamStore: MemoryStreamStore[ObjectLocation]
  ): ObjectLocation =
    ObjectLocation(randomAlphanumeric, randomAlphanumeric)

  override def createObject(
    location: ObjectLocation,
    contents: String
  )(implicit streamStore: MemoryStreamStore[ObjectLocation]): Unit = {
    val inputStream = stringCodec.toStream(contents).right.value
    streamStore.put(location)(inputStream)
  }
}

class S3SizeFinderTest
    extends SizeFinderTestCases[S3ObjectLocation, Bucket]
    with S3Fixtures {
  override def withContext[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { testWith }

  override def withSizeFinder[R](
    testWith: TestWith[SizeFinder[S3ObjectLocation], R]
  )(implicit bucket: Bucket): R =
    testWith(
      new S3SizeFinder()
    )

  override def createIdent(implicit bucket: Bucket): S3ObjectLocation =
    createObjectLocationWith(bucket)

  override def createObject(
    location: S3ObjectLocation,
    contents: String
  )(implicit context: Bucket): Unit =
    s3Client.putObject(location.bucket, location.key, contents)

  it("fails if the prefix is for a non-existent S3 bucket") {
    val finder = new S3SizeFinder()

    val result = finder.getSize(
      createObjectLocationWith(bucket = createBucket)
    )

    result.left.value.e shouldBe a[AmazonS3Exception]
  }
}
