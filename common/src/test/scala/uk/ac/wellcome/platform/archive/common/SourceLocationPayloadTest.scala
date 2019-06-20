package uk.ac.wellcome.platform.archive.common

import java.time.Instant

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.ingests.models._
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators

class SourceLocationPayloadTest
    extends FunSpec
    with Matchers
    with RandomThings
    with ObjectLocationGenerators {

  it("creates a payload from an ingest") {
    val ingestId = createIngestID
    val ingestType = CreateIngestType
    val sourceLocation = createObjectLocation
    val space = randomAlphanumeric()
    val ingestDate = Instant.now()

    val ingest = Ingest(
      id = ingestId,
      ingestType = ingestType,
      sourceLocation = StorageLocation(
        provider = StandardStorageProvider,
        location = sourceLocation
      ),
      space = Namespace(space),
      createdDate = ingestDate
    )

    val expectedPayload = SourceLocationPayload(
      context = PipelineContext(
        ingestId = ingestId,
        ingestType = ingestType,
        storageSpace = StorageSpace(space),
        ingestDate = ingestDate
      ),
      sourceLocation = sourceLocation
    )

    SourceLocationPayload(ingest) shouldBe expectedPayload
  }
}
