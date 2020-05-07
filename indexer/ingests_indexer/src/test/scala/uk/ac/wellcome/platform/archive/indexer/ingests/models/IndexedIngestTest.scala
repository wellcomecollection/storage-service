package uk.ac.wellcome.platform.archive.indexer.ingests.models

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators

class IndexedIngestTest extends AnyFunSpec with Matchers with IngestGenerators {
  it("doesn't set a failureDescription on successful ingests") {
    val ingest = createIngestWith(
      events = Seq(
        createIngestEventWith("Unpacking started"),
        createIngestEventWith("Unpacking succeeded"),
      )
    )

    IndexedIngest(ingest).failureDescriptions shouldBe None
  }

  it("sets a failureDescription on failed ingests") {
    val ingest = createIngestWith(
      events = Seq(
        createIngestEventWith("Unpacking started"),
        createIngestEventWith("Unpacking failed"),
      )
    )

    IndexedIngest(ingest).failureDescriptions shouldBe Some("Unpacking failed")
  }

  it("combines multiple failure messages") {
    val ingest = createIngestWith(
      events = Seq(
        createIngestEventWith("Unpacking started"),
        createIngestEventWith("Unpacking failed"),
        createIngestEventWith("Replicating failed"),
      )
    )

    IndexedIngest(ingest).failureDescriptions shouldBe Some("Unpacking failed, Replicating failed")
  }
}
