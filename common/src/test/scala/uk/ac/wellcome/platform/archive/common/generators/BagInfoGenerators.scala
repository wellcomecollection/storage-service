package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.bagit.models
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagInfo, ExternalDescription, ExternalIdentifier, PayloadOxum}
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings

trait BagInfoGenerators
    extends ExternalIdentifierGenerators
    with StorageRandomThings {

  def createBagInfoWith(
    payloadOxum: PayloadOxum = randomPayloadOxum,
    externalIdentifier: ExternalIdentifier = createExternalIdentifier,
    externalDescription: Option[ExternalDescription] = Some(
      randomExternalDescription)
  ): BagInfo =
    models.BagInfo(
      externalIdentifier = externalIdentifier,
      payloadOxum = payloadOxum,
      baggingDate = randomLocalDate,
      sourceOrganisation = Some(randomSourceOrganisation),
      externalDescription = externalDescription,
      internalSenderIdentifier = Some(randomInternalSenderIdentifier),
      internalSenderDescription = Some(randomInternalSenderDescription)
    )

  def createBagInfo: BagInfo = createBagInfoWith()
}
