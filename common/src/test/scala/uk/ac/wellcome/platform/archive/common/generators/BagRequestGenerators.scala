package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.models.BagRequest
import uk.ac.wellcome.platform.archive.common.models.bagit.BagLocation

trait BagRequestGenerators extends RandomThings {
  def createBagRequestWith(bagLocation: BagLocation): BagRequest =
    BagRequest(
      archiveRequestId = randomUUID,
      bagLocation = bagLocation
    )
}
