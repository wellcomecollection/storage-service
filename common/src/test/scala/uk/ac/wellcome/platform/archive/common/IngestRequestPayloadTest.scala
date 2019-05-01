package uk.ac.wellcome.platform.archive.common

import java.time.Instant

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Ingest,
  Namespace,
  StandardStorageProvider,
  StorageLocation
}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.fixtures.S3

class IngestRequestPayloadTest
    extends FunSpec
    with Matchers
    with RandomThings
    with S3 {
  it("creates a payload from an ingest") {
    val ingestId = createIngestID
    val sourceLocation = createObjectLocation
    val space = randomAlphanumeric()
    val ingestDate = Instant.now()

    val ingest = Ingest(
      id = ingestId,
      sourceLocation = StorageLocation(
        provider = StandardStorageProvider,
        location = sourceLocation
      ),
      space = Namespace(space),
      createdDate = ingestDate
    )

    val expectedPayload = IngestRequestPayload(
      ingestId = ingestId,
      ingestDate = ingestDate,
      storageSpace = StorageSpace(space),
      sourceLocation = sourceLocation
    )

    IngestRequestPayload(ingest) shouldBe expectedPayload
  }
}
