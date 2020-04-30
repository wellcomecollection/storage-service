package uk.ac.wellcome.platform.archive.common.ingests.models

import java.time.Instant

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators

class IngestTest extends AnyFunSpec with Matchers with IngestGenerators {
  describe("sets the last modified date correctly") {
    it("if there are no events, last modified date is None") {
      val ingest = createIngestWith(events = Seq.empty)

      ingest.lastModifiedDate shouldBe None
    }

    it("if there are events, last modified date is the latest") {
      val events = Seq(1, 5, 3, 4, 2).map { t =>
        createIngestEventWith(
          createdDate = Instant.ofEpochSecond(t)
        )
      }

      val ingest = createIngestWith(events = events)

      ingest.lastModifiedDate shouldBe Some(Instant.ofEpochSecond(5))
    }
  }
}
