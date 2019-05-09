package uk.ac.wellcome.platform.archive.common

import org.scalatest.FunSpec
import org.scalatest.concurrent.ScalaFutures
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3

import scala.concurrent.ExecutionContext.Implicits.global

class StreamableTest
    extends FunSpec
    with S3
    with ScalaFutures
    with RandomThings {

  import uk.ac.wellcome.platform.archive.common.storage.Streamable._

  implicit val _s3Client = s3Client

  describe("converts to a Try[InputStream]") {
    it("produces a failure from an invalid ObjectLocation") {
      val inputStreamFuture = ObjectLocation(
        "invalid_bucket",
        "invalid.key"
      ).toInputStream

      whenReady(inputStreamFuture.failed) { e =>
        e.getMessage should include("The specified bucket is not valid")
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
