package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.models.BagRequest
import uk.ac.wellcome.platform.archive.common.models.bagit.BagLocation

trait BagRequestGenerators extends RandomThings with BagLocationGenerators {
  def createBagRequest(): BagRequest = createBagRequestWith()

  def createBagRequestWith(
    bagLocation: BagLocation = createBagLocation()): BagRequest =
    BagRequest(
      requestId = randomUUID,
      bagLocation = bagLocation
    )
}
