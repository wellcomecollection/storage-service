package uk.ac.wellcome.platform.archive.bagverifier.fixity

import java.net.URI

import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.bagverifier.fixity.memory.MemoryFixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.generators.FixityGenerators
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
          updateFunction: Map[String, String] => Either[
            UpdateFunctionError,
            Map[String, String]
          ]
        ): Either[UpdateError, Map[String, String]] =
          Left(UpdateWriteError(StoreWriteError(new Throwable("BOOM!"))))

        override def get(
          location: ObjectLocation
        ): Either[ReadError, Map[String, String]] =
          super.get(location) match {
            case Right(tags)                => Right(tags)
            case Left(_: DoesNotExistError) => Right(Map[String, String]())
            case Left(err)                  => Left(err)
          }
      }

      val streamStore = MemoryStreamStore[ObjectLocation]()

      val location = createObjectLocation

      val (content, checksum) = createStringChecksumPair
      val inputStream = stringCodec.toStream(content).right.value
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
      val brokenTags = new MemoryTags[ObjectLocation](initialTags = Map.empty) {
        private var calls: Int = 0

        override def get(
          location: ObjectLocation
        ): Either[ReadError, Map[String, String]] = {
          if (calls == 0) {
            calls += 1
            Right(Map[String, String]())
          } else {
            Right(
              Map(
                "Content-MD5" -> "md5.wrong",
                "Content-SHA1" -> "sha1.wrong",
                "Content-SHA256" -> "sha256.wrong"
              )
            )
          }
        }
      }

      val streamStore = MemoryStreamStore[ObjectLocation]()

      val location = createObjectLocation

      val (content, checksum) = createStringChecksumPair
      val inputStream = stringCodec.toStream(content).right.value
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
      result
        .asInstanceOf[FileFixityCouldNotWriteTag]
        .e shouldBe a[AssertionError]
    }
  }
}
