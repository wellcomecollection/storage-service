package uk.ac.wellcome.platform.archive.common.bagit.models

import org.apache.commons.io.IOUtils
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.bagit.models
import uk.ac.wellcome.platform.archive.common.bagit.models.error.InvalidBagInfo
import uk.ac.wellcome.platform.archive.common.generators.{
  ExternalIdentifierGenerators,
  PayloadOxumGenerators
}

class BagInfoTest
    extends FunSpec
    with Matchers
    with ExternalIdentifierGenerators
    with PayloadOxumGenerators {
  it("extracts a BagInfo object from a bagInfo file with only required fields") {
    val externalIdentifier = createExternalIdentifier
    val payloadOxum = createPayloadOxum
    val baggingDate = randomLocalDate

    val bagInfoString =
      s"""|External-Identifier: $externalIdentifier
          |Payload-Oxum: ${payloadOxum.payloadBytes}.${payloadOxum.numberOfPayloadFiles}
          |Bagging-Date: $baggingDate
          |""".stripMargin

    BagInfo.parseBagInfo(IOUtils.toInputStream(bagInfoString, "UTF-8")) shouldBe Right(
      BagInfo(externalIdentifier, payloadOxum, baggingDate)
    )
  }

  it(
    "extracts a BagInfo object from a bagInfo file with all required and optional fields"
  ) {
    val externalIdentifier = createExternalIdentifier
    val payloadOxum = createPayloadOxum
    val baggingDate = randomLocalDate
    val sourceOrganisation = Some(randomSourceOrganisation)
    val externalDescription = Some(randomExternalDescription)
    val internalSenderIdentifier = Some(randomInternalSenderIdentifier)
    val internalSenderDescription = Some(randomInternalSenderDescription)

    val bagInfoString =
      s"""|External-Identifier: $externalIdentifier
          |Payload-Oxum: ${payloadOxum.payloadBytes}.${payloadOxum.numberOfPayloadFiles}
          |Bagging-Date: $baggingDate
          |Source-Organization: ${sourceOrganisation.get}
          |External-Description: ${externalDescription.get}
          |Internal-Sender-Identifier: ${internalSenderIdentifier.get}
          |Internal-Sender-Description: ${internalSenderDescription.get}
          |""".stripMargin

    BagInfo.parseBagInfo(IOUtils.toInputStream(bagInfoString, "UTF-8")) shouldBe Right(
      models.BagInfo(
        externalIdentifier,
        payloadOxum,
        baggingDate,
        sourceOrganisation,
        externalDescription,
        internalSenderIdentifier,
        internalSenderDescription
      )
    )
  }

  it(
    "returns a left of invalid bag info error if there is no external identifier in bag-info.txt"
  ) {
    val bagInfoString =
      s"""|Source-Organization: $randomSourceOrganisation
          |Payload-Oxum: ${createPayloadOxum.payloadBytes}.${createPayloadOxum.numberOfPayloadFiles}
          |Bagging-Date: $randomLocalDate""".stripMargin

    BagInfo.parseBagInfo(IOUtils.toInputStream(bagInfoString, "UTF-8")) shouldBe Left(
      InvalidBagInfo(List("External-Identifier"))
    )
  }

  it(
    "returns a left of invalid bag info error if there is no payload-oxum in bag-info.txt"
  ) {
    val bagInfoString =
      s"""|External-Identifier: $createExternalIdentifier
          |Source-Organization: $randomSourceOrganisation
          |Bagging-Date: $randomLocalDate""".stripMargin

    BagInfo.parseBagInfo(IOUtils.toInputStream(bagInfoString, "UTF-8")) shouldBe Left(
      InvalidBagInfo(List("Payload-Oxum"))
    )
  }

  it(
    "returns a left of invalid bag info error if the payload-oxum is invalid in bag-info.txt"
  ) {
    val bagInfoString =
      s"""|External-Identifier: $createExternalIdentifier
          |Source-Organization: $randomSourceOrganisation
          |Payload-Oxum: sgadfjag
          |Bagging-Date: $randomLocalDate""".stripMargin

    BagInfo.parseBagInfo(IOUtils.toInputStream(bagInfoString, "UTF-8")) shouldBe Left(
      InvalidBagInfo(List("Payload-Oxum"))
    )
  }

  it(
    "returns a left of invalid bag info error if there is no bagging date in bag-info.txt"
  ) {
    val bagInfoString =
      s"""|External-Identifier: $createExternalIdentifier
          |Source-Organization: $randomSourceOrganisation
          |Payload-Oxum: ${createPayloadOxum.payloadBytes}.${createPayloadOxum.numberOfPayloadFiles}""".stripMargin

    BagInfo.parseBagInfo(IOUtils.toInputStream(bagInfoString, "UTF-8")) shouldBe Left(
      InvalidBagInfo(List("Bagging-Date"))
    )
  }

  it(
    "returns a left of invalid bag info error if the bagging date is invalid in bag-info.txt"
  ) {
    val bagInfoString =
      s"""|External-Identifier: $createExternalIdentifier
          |Source-Organization: $randomSourceOrganisation
          |Payload-Oxum: ${createPayloadOxum.payloadBytes}.${createPayloadOxum.numberOfPayloadFiles}
          |Bagging-Date: sdfkjghl""".stripMargin

    BagInfo.parseBagInfo(IOUtils.toInputStream(bagInfoString, "UTF-8")) shouldBe Left(
      InvalidBagInfo(List("Bagging-Date"))
    )
  }

  it("returns a left of invalid bag info error if bag-info.txt is empty") {
    val bagInfoString = ""

    BagInfo.parseBagInfo(IOUtils.toInputStream(bagInfoString, "UTF-8")) shouldBe Left(
      InvalidBagInfo(List("External-Identifier", "Payload-Oxum", "Bagging-Date"))
    )
  }

}
