package uk.ac.wellcome.platform.archive.common.storage.services

import com.amazonaws.services.s3.model.AmazonS3Exception
import org.scalatest.{EitherValues, FunSpec, Matchers, TryValues}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators
import uk.ac.wellcome.storage.store.memory.MemoryStreamStore
import uk.ac.wellcome.storage.streaming.Codec._
import uk.ac.wellcome.storage.streaming.InputStreamWithLengthAndMetadata

import scala.util.Failure

trait SizeFinderTestCases[Context]
    extends FunSpec
    with Matchers
    with TryValues {
  def withContext[R](testWith: TestWith[Context, R]): R

  def withSizeFinder[R](testWith: TestWith[SizeFinder, R])(
    implicit context: Context): R

  def createLocation(implicit context: Context): ObjectLocation

  def createObjectAtLocation(location: ObjectLocation, contents: String)(
    implicit context: Context): Unit

  describe("it behaves as a size finder") {
    it("finds the sizes of objects in a prefix") {
      withContext { implicit context =>
        val location = createLocation
        createObjectAtLocation(location, "the quick brown fox")

        val result = withSizeFinder {
          _.getSize(location)
        }

        result.success.value shouldBe 19L
      }
    }

    it("returns an empty list if there's nothing under the prefix") {
      withContext { implicit context =>
        val location = createLocation

        val result = withSizeFinder {
          _.getSize(location)
        }

        result shouldBe a[Failure[_]]
      }
    }
  }
}

class MemorySizeFinderTest
    extends SizeFinderTestCases[MemoryStreamStore[ObjectLocation]]
    with ObjectLocationGenerators
    with EitherValues {
  override def withContext[R](
    testWith: TestWith[MemoryStreamStore[ObjectLocation], R]): R =
    testWith(MemoryStreamStore[ObjectLocation]())

  override def withSizeFinder[R](testWith: TestWith[SizeFinder, R])(
    implicit streamStore: MemoryStreamStore[ObjectLocation]): R =
    testWith(
      new MemorySizeFinder(streamStore.memoryStore)
    )

  override def createLocation(
    implicit streamStore: MemoryStreamStore[ObjectLocation]): ObjectLocation =
    createObjectLocation

  override def createObjectAtLocation(location: ObjectLocation,
                                      contents: String)(
    implicit streamStore: MemoryStreamStore[ObjectLocation]): Unit = {
    val is = stringCodec.toStream(contents).right.value
    streamStore.put(location)(
      InputStreamWithLengthAndMetadata(is, metadata = Map.empty)
    )
  }
}

class S3SizeFinderTest
    extends SizeFinderTestCases[Bucket]
    with S3Fixtures
    with EitherValues {
  override def withContext[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { testWith }

  override def withSizeFinder[R](testWith: TestWith[SizeFinder, R])(
    implicit bucket: Bucket): R =
    testWith(
      new S3SizeFinder()
    )

  override def createLocation(implicit bucket: Bucket): ObjectLocation =
    createObjectLocationWith(bucket)

  override def createObjectAtLocation(
    location: ObjectLocation,
    contents: String)(implicit context: Bucket): Unit =
    s3Client.putObject(
      location.namespace,
      location.path,
      contents
    )

  it("fails if the prefix is for a non-existent S3 bucket") {
    val finder = new S3SizeFinder()

    val result = finder.getSize(createObjectLocation)

    result.failed.get shouldBe a[AmazonS3Exception]
  }
}
