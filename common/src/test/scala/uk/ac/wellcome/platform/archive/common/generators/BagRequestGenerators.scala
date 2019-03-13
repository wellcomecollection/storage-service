package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.bagit.models.BagLocation
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.ingests.models.BagRequest

trait BagRequestGenerators extends RandomThings with BagLocationGenerators {
  def createBagRequest(): BagRequest = createBagRequestWith()

  def createBagRequestWith(
    bagLocation: BagLocation = createBagLocation()): BagRequest =
    BagRequest(
      requestId = randomUUID,
      bagLocation = bagLocation
    )
}
