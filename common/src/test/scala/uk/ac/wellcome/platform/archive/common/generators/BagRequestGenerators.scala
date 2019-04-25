package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.bagit.models.BagLocation
import uk.ac.wellcome.platform.archive.common.ingests.models.BagRequest

trait BagRequestGenerators extends BagLocationGenerators {
  def createBagRequest(): BagRequest = createBagRequestWith()

  def createBagRequestWith(
    bagLocation: BagLocation = createBagLocation()): BagRequest =
    BagRequest(
      ingestId = createIngestID,
      bagLocation = bagLocation
    )
}
