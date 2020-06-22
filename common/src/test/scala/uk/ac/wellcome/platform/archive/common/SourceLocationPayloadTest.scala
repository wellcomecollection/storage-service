package uk.ac.wellcome.platform.archive.common

import java.time.Instant

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.common.generators.{ExternalIdentifierGenerators, StorageSpaceGenerators}
import uk.ac.wellcome.platform.archive.common.ingests.models._
import uk.ac.wellcome.storage.ObjectLocation

class SourceLocationPayloadTest
    extends AnyFunSpec
    with Matchers
    with ExternalIdentifierGenerators
    with StorageSpaceGenerators {

  it("creates a payload from an ingest") {
    val ingestId = createIngestID
    val ingestType = CreateIngestType
    val sourceLocation = ObjectLocation(randomAlphanumeric, randomAlphanumeric)
    val space = createStorageSpace
    val ingestDate = Instant.now()
    val externalIdentifier = createExternalIdentifier

    val ingest = Ingest(
      id = ingestId,
      ingestType = ingestType,
      sourceLocation = SourceLocation(
        provider = AmazonS3StorageProvider,
        location = sourceLocation
      ),
      space = space,
      createdDate = ingestDate,
      externalIdentifier = externalIdentifier,
      callback = None,
      status = Ingest.Accepted
    )

    val expectedPayload = SourceLocationPayload(
      context = PipelineContext(
        ingestId = ingestId,
        ingestType = ingestType,
        storageSpace = space,
        ingestDate = ingestDate,
        externalIdentifier = externalIdentifier
      ),
      sourceLocation = sourceLocation
    )

    SourceLocationPayload(ingest) shouldBe expectedPayload
  }
}
