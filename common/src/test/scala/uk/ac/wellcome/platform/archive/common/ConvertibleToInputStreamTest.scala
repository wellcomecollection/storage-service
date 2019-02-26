package uk.ac.wellcome.platform.archive.common

import org.scalatest.FunSpec
import org.scalatest.concurrent.ScalaFutures
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3

import scala.concurrent.ExecutionContext.Implicits.global

class ConvertibleToInputStreamTest
    extends FunSpec
    with S3
    with ScalaFutures
    with RandomThings {

  implicit val _ = s3Client

  import uk.ac.wellcome.platform.archive.common.ConvertibleToInputStream._

  describe("converts to a Try[InputStream]") {
    it("produces a failure from an invalid ObjectLocation") {
      val inputStreamFuture = ObjectLocation(
        "invalid_bucket",
        "invalid.key"
      ).toInputStream

      whenReady(inputStreamFuture.failed) { e =>
        e shouldBe a[RuntimeException]
      }
    }

    it("produces a success from an valid ObjectLocation") {
      withLocalS3Bucket { bucket =>
        val key = randomAlphanumeric()
        val content = randomAlphanumeric()

        s3Client.putObject(bucket.name, key, content)

        val inputStreamFuture = ObjectLocation(
          bucket.name,
          key
        ).toInputStream

        whenReady(inputStreamFuture) { inputStream =>
          scala.io.Source
            .fromInputStream(
              inputStream
            )
            .mkString shouldEqual content
        }
      }
    }
  }
}
