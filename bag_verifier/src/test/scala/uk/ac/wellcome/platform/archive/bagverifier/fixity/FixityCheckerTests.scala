package uk.ac.wellcome.platform.archive.bagverifier.fixity

import java.net.URI

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.bagverifier.fixity.memory.MemoryFixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.generators.FixityGenerators
import uk.ac.wellcome.platform.archive.common.storage.{LocateFailure, LocationParsingError}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.store.memory.MemoryStreamStore

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
  }
}
