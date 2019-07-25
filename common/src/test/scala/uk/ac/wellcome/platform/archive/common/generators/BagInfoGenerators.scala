package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.bagit.models
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagInfo,
  ExternalDescription,
  ExternalIdentifier,
  PayloadOxum
}

trait BagInfoGenerators
    extends ExternalIdentifierGenerators
    with PayloadOxumGenerators {

  def createBagInfoWith(
    payloadOxum: PayloadOxum = createPayloadOxum,
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
