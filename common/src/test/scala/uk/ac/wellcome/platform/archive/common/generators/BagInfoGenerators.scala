package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.models.bagit._

trait BagInfoGenerators extends ExternalIdentifierGenerators with RandomThings {

  def createBagInfoWith(
    externalIdentifier: ExternalIdentifier = createExternalIdentifier,
    externalDescription: Option[ExternalDescription] = Some(
      randomExternalDescription)
  ): BagInfo =
    BagInfo(
      externalIdentifier = externalIdentifier,
      payloadOxum = randomPayloadOxum,
      baggingDate = randomLocalDate,
      sourceOrganisation = Some(randomSourceOrganisation),
      externalDescription = externalDescription,
      internalSenderIdentifier = Some(randomInternalSenderIdentifier),
      internalSenderDescription = Some(randomInternalSenderDescription)
    )

  def createBagInfo: BagInfo = createBagInfoWith()
}
