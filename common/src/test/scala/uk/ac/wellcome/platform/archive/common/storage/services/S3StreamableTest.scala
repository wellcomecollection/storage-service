package uk.ac.wellcome.platform.archive.common.storage.services

import com.amazonaws.services.s3.model.AmazonS3Exception
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, TryValues}
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.storage.fixtures.S3

import scala.util.{Failure, Success}

class S3StreamableTest
  extends FunSpec
    with S3
    with ScalaFutures
    with TryValues
    with RandomThings {

  import S3StreamableInstances._

  describe("converts to a Try[InputStream]") {
    it("produces a failure from an invalid ObjectLocation") {
      val result = createObjectLocation.toInputStream

      result shouldBe a[Failure[_]]
      result.failed.get shouldBe a[AmazonS3Exception]
      result.failed.get.getMessage should startWith("The specified bucket does not exist")
    }

    it("produces a success from an valid ObjectLocation") {
      withLocalS3Bucket { bucket =>
        val content = randomAlphanumeric()

        val location = createObjectLocationWith(bucket)
        createObject(location, content = content)

        val result = location.toInputStream

        result shouldBe a[Success[_]]
        val inputStream = result.get

        scala.io.Source.fromInputStream(inputStream).mkString shouldEqual content
      }
    }
  }
}
