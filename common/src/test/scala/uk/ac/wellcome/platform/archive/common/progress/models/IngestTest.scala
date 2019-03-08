package uk.ac.wellcome.platform.archive.common.progress.models

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.generators.ProgressGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.common.progress.fixtures.TimeTestFixture

class IngestTest
    extends FunSpec
    with Matchers
    with TimeTestFixture
    with ProgressGenerators
    with RandomThings {

  it("can be created") {
    val progress = createProgress
    progress.status shouldBe Ingest.Accepted
    assertRecent(progress.createdDate)
    progress.lastModifiedDate shouldBe progress.createdDate
    progress.events shouldBe List.empty
  }

  import org.scalatest.prop.TableDrivenPropertyChecks._

  private val progressStatus = Table(
    ("string-status", "parsed-status"),
    ("accepted", Ingest.Accepted),
    ("processing", Ingest.Processing),
    ("succeeded", Ingest.Completed),
    ("failed", Ingest.Failed),
  )

  it("converts all callback status values to strings") {
    forAll(progressStatus) { (statusString, status) =>
      createProgressWith(status = status).status.toString shouldBe statusString
    }
  }
}
