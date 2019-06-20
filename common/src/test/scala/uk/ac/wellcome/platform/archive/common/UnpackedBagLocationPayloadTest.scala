package uk.ac.wellcome.platform.archive.common

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators

class UnpackedBagLocationPayloadTest
    extends FunSpec
    with Matchers
    with PayloadGenerators {
  it("creates a payload from an ingest payload and an unpacked bag location") {
    val sourceLocation = createObjectLocation
    val unpackedBagLocation = createObjectLocation
    val storageSpace = createStorageSpace

    val sourceLocationPayload = createSourceLocationPayloadWith(
      sourceLocation = sourceLocation,
      storageSpace = storageSpace
    )

    val expectedPayload = UnpackedBagLocationPayload(
      context = PipelineContext(
        ingestId = sourceLocationPayload.ingestId,
        ingestType = sourceLocationPayload.ingestType,
        storageSpace = storageSpace,
        ingestDate = sourceLocationPayload.ingestDate
      ),
      unpackedBagLocation = unpackedBagLocation
    )

    UnpackedBagLocationPayload(sourceLocationPayload, unpackedBagLocation) shouldBe expectedPayload
  }
}
