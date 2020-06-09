package uk.ac.wellcome.platform.archive.bagverifier.fixity

import java.io.FilterInputStream
import java.net.URI

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.bagverifier.fixity.memory.MemoryFixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.generators.FixityGenerators
import uk.ac.wellcome.platform.archive.common.storage.{LocateFailure, LocationParsingError}
import uk.ac.wellcome.storage.{Identified, ObjectLocation}
import uk.ac.wellcome.storage.store.memory.{MemoryStore, MemoryStreamStore}
import uk.ac.wellcome.storage.streaming.InputStreamWithLength

class FixityCheckerTests extends AnyFunSpec with Matchers with FixityGenerators {
  override def resolve(location: ObjectLocation): URI =
    new URI(s"mem://${location.namespace}/${location.path}")

  describe("handles errors correctly") {
    it("turns an error in locate() into a FileFixityCouldNotRead") {
      val streamStore = MemoryStreamStore[ObjectLocation]()

      val brokenChecker = new MemoryFixityChecker(streamStore) {
        override def locate(uri: URI): Either[LocateFailure[URI], ObjectLocation] =
          Left(LocationParsingError(uri, msg = "BOOM!"))
      }

      val expectedFileFixity = createExpectedFileFixity
      brokenChecker.check(expectedFileFixity) shouldBe a[FileFixityCouldNotRead]
    }

    it("handles an error when trying to checksum the object") {
      val badStream = new FilterInputStream(randomInputStream()) {
        override def read(b: Array[Byte], off: Int, len: Int): Int =
          throw new Throwable("BOOM!")
      }

      val closedStream = new InputStreamWithLength(
        badStream,
        length = randomInt(from = 1, to = 10)
      )

      closedStream.close()

      val streamStore = new MemoryStreamStore[ObjectLocation](
        memoryStore = new MemoryStore[ObjectLocation, Array[Byte]](initialEntries = Map.empty)
      ) {
        override def get(location: ObjectLocation): this.ReadEither =
          Right(Identified(location, closedStream))
      }

      val expectedFileFixity = createExpectedFileFixityWith(length = None)

      val checker = new MemoryFixityChecker(streamStore)

      checker.check(expectedFileFixity) shouldBe a[FileFixityCouldNotGetChecksum]
    }
  }
}
