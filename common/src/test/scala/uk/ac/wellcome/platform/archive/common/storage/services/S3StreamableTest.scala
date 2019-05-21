package uk.ac.wellcome.platform.archive.common.storage.services

import java.nio.file.Paths

import org.scalatest.{FunSpec, TryValues}
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.storage.Resolvable
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3

class S3StreamableTest
    extends FunSpec
    with S3
    with TryValues
    with RandomThings {

  import StreamableInstances._

  case class Thing(stuff: String)

  implicit val thingResolver: Resolvable[Thing] = new Resolvable[Thing] {
    override def resolve(root: ObjectLocation)(thing: Thing): ObjectLocation = {
      val paths = Paths.get(root.key, thing.stuff)
      root.copy(key = paths.toString)
    }
  }

  describe("converts to a Try[InputStream]") {
    it("produces a failure from an invalid root") {

      val invalidRoot = ObjectLocation(
        "invalid_bucket",
        "invalid.key"
      )

      val myThing = Thing(randomAlphanumeric())

      val myStream = myThing.from(invalidRoot)

      myStream.failed.get.getMessage should include(
        "The specified bucket is not valid")
    }

    it("produces a success from a valid ObjectLocation") {
      withLocalS3Bucket { bucket =>
        val key = randomAlphanumeric()
        val thingStuff = randomAlphanumeric()

        s3Client.putObject(bucket.name, s"$key/$thingStuff", thingStuff)

        val validRoot = ObjectLocation(
          bucket.name,
          key
        )

        val myThing = Thing(thingStuff)

        val inputStream = myThing.from(validRoot).get

        scala.io.Source
          .fromInputStream(inputStream)
          .mkString shouldEqual thingStuff
      }
    }
  }
}
