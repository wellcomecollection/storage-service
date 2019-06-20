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
    val sourceLocation = createObjectLocation
    val space = randomAlphanumeric()
    val ingestDate = Instant.now()

    val ingest = Ingest(
      id = ingestId,
      ingestType = CreateIngestType,
      sourceLocation = StorageLocation(
        provider = StandardStorageProvider,
        location = sourceLocation
      ),
      space = Namespace(space),
      createdDate = ingestDate
    )

    val expectedPayload = SourceLocationPayload(
      ingestId = ingestId,
      ingestDate = ingestDate,
      storageSpace = StorageSpace(space),
      sourceLocation = sourceLocation
    )

    SourceLocationPayload(ingest) shouldBe expectedPayload
  }
}
