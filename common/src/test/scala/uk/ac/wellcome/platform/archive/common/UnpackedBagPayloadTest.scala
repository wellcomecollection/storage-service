package uk.ac.wellcome.platform.archive.common

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators

class UnpackedBagPayloadTest
    extends FunSpec
    with Matchers
    with PayloadGenerators {
  it("creates a payload from an ingest payload and an unpacked bag location") {
    val sourceLocation = createObjectLocation
    val unpackedBagLocation = createObjectLocation
    val storageSpace = createStorageSpace

    val ingestRequestPayload = createSourceLocationPayloadWith(
      sourceLocation = sourceLocation,
      storageSpace = storageSpace
    )

    val expectedPayload = UnpackedBagPayload(
      ingestId = ingestRequestPayload.ingestId,
      ingestDate = ingestRequestPayload.ingestDate,
      storageSpace = storageSpace,
      unpackedBagLocation = unpackedBagLocation
    )

    UnpackedBagPayload(ingestRequestPayload, unpackedBagLocation) shouldBe expectedPayload
  }
}
