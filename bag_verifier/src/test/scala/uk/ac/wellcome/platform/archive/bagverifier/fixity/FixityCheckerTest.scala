package uk.ac.wellcome.platform.archive.bagverifier.fixity

import java.net.URI

import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.bagverifier.fixity.memory.MemoryFixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.generators.FixityGenerators
import uk.ac.wellcome.platform.archive.common.verify.{Checksum, ChecksumValue, MD5}
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.store.memory.MemoryStreamStore
import uk.ac.wellcome.storage.streaming.Codec.stringCodec
import uk.ac.wellcome.storage.tags.memory.MemoryTags

class FixityCheckerTest
  extends AnyFunSpec
    with Matchers
    with EitherValues
    with FixityGenerators {
  override def resolve(location: ObjectLocation): URI =
    new URI(s"mem://${location.namespace}/${location.path}")

  describe("handling errors") {
    it("errors if it cannot write tags") {
      val brokenTags = new MemoryTags[ObjectLocation](initialTags = Map.empty) {
        override def update(location: ObjectLocation)(
          updateFunction:
            Map[String, String] => Either[UpdateFunctionError, Map[String, String]]):
          Either[UpdateError, Map[String, String]] =
          Left(UpdateWriteError(StoreWriteError(new Throwable("BOOM!"))))
      }

      val streamStore = MemoryStreamStore[ObjectLocation]()

      val location = createObjectLocation

      // md5("HelloWorld")
      val checksum = Checksum(
        algorithm = MD5,
        value = ChecksumValue("68e109f0f40ca72a15e05cc22786f8e6")
      )
      val inputStream = stringCodec.toStream("Hello world").right.value
      streamStore.put(location)(inputStream) shouldBe a[Right[_, _]]

      val fixityChecker = new MemoryFixityChecker(
        streamStore = streamStore,
        tags = brokenTags
      )

      val expectedFileFixity = createExpectedFileFixityWith(
        location = location,
        checksum = checksum
      )

      val result = fixityChecker.check(expectedFileFixity)

      result shouldBe a[FileFixityCouldNotWriteTag]
    }

    it("errors if a new tag is written between initial read and write") {
      true shouldBe false
    }
  }
}
