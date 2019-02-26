package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.models.BagRequest
import uk.ac.wellcome.platform.archive.common.models.bagit.BagLocation

trait BagRequestGenerators extends BagLocationGenerators {
  def createBagRequestWith(
    bagLocation: BagLocation = createBagLocation): BagRequest =
    BagRequest(
      archiveRequestId = randomUUID,
      bagLocation = bagLocation
    )

  def createBagRequest: BagRequest =
    createBagRequestWith()
}
