package uk.ac.wellcome.platform.archive.common.ingests.models

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingest.fixtures.TimeTestFixture

class IngestTest
    extends FunSpec
    with Matchers
    with TimeTestFixture
    with IngestGenerators
    with RandomThings {

  it("can be created") {
    val ingest = createIngest
    ingest.status shouldBe Ingest.Accepted
    assertRecent(ingest.createdDate)
    ingest.lastModifiedDate shouldBe ingest.createdDate
    ingest.events shouldBe List.empty
  }

  import org.scalatest.prop.TableDrivenPropertyChecks._

  private val ingestStatus = Table(
    ("string-status", "parsed-status"),
    ("accepted", Ingest.Accepted),
    ("processing", Ingest.Processing),
    ("succeeded", Ingest.Completed),
    ("failed", Ingest.Failed),
  )

  it("converts all callback status values to strings") {
    forAll(ingestStatus) { (statusString, status) =>
      createIngestWith(status = status).status.toString shouldBe statusString
    }
  }
}
