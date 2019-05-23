package uk.ac.wellcome.platform.archive.common.bagit.models

import org.apache.commons.io.IOUtils
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.bagit.models
import uk.ac.wellcome.platform.archive.common.bagit.models.error.InvalidBagInfo
import uk.ac.wellcome.platform.archive.common.fixtures.BagIt
import uk.ac.wellcome.platform.archive.common.generators.ExternalIdentifierGenerators

class BagInfoTest
    extends FunSpec
    with BagIt
    with Matchers
    with ExternalIdentifierGenerators {
  case class Thing(id: String)
  val t = Thing("a")

  it("extracts a BagInfo object from a bagInfo file with only required fields") {
    val externalIdentifier = createExternalIdentifier
    val payloadOxum = randomPayloadOxum
    val baggingDate = randomLocalDate
    val bagInfoString =
      bagInfoFileContents(externalIdentifier, payloadOxum, baggingDate)

    BagInfo.parseBagInfo(t, IOUtils.toInputStream(bagInfoString, "UTF-8")) shouldBe Right(
      BagInfo(externalIdentifier, payloadOxum, baggingDate))
  }

  it(
    "extracts a BagInfo object from a bagInfo file with all required and optional fields") {
    val externalIdentifier = createExternalIdentifier
    val payloadOxum = randomPayloadOxum
    val baggingDate = randomLocalDate
    val sourceOrganisation = Some(randomSourceOrganisation)
    val externalDescription = Some(randomExternalDescription)
    val internalSenderIdentifier = Some(randomInternalSenderIdentifier)
    val internalSenderDescription = Some(randomInternalSenderDescription)

    val bagInfoString = bagInfoFileContents(
      externalIdentifier,
      payloadOxum,
      baggingDate,
      sourceOrganisation,
      externalDescription,
      internalSenderIdentifier,
      internalSenderDescription)

    BagInfo.parseBagInfo(t, IOUtils.toInputStream(bagInfoString, "UTF-8")) shouldBe Right(
      models.BagInfo(
        externalIdentifier,
        payloadOxum,
        baggingDate,
        sourceOrganisation,
        externalDescription,
        internalSenderIdentifier,
        internalSenderDescription))
  }

  it(
    "returns a left of invalid bag info error if there is no external identifier in bag-info.txt") {
    val bagInfoString =
      s"""|Source-Organization: $randomSourceOrganisation
          |Payload-Oxum: ${randomPayloadOxum.payloadBytes}.${randomPayloadOxum.numberOfPayloadFiles}
          |Bagging-Date: $randomLocalDate""".stripMargin

    BagInfo.parseBagInfo(t, IOUtils.toInputStream(bagInfoString, "UTF-8")) shouldBe Left(
      InvalidBagInfo(t, List("External-Identifier")))
  }

  it(
    "returns a left of invalid bag info error if there is no payload-oxum in bag-info.txt") {
    val bagInfoString =
      s"""|External-Identifier: $createExternalIdentifier
          |Source-Organization: $randomSourceOrganisation
          |Bagging-Date: $randomLocalDate""".stripMargin

    BagInfo.parseBagInfo(t, IOUtils.toInputStream(bagInfoString, "UTF-8")) shouldBe Left(
      InvalidBagInfo(t, List("Payload-Oxum")))
  }

  it(
    "returns a left of invalid bag info error if the payload-oxum is invalid in bag-info.txt") {
    val bagInfoString =
      s"""|External-Identifier: $createExternalIdentifier
          |Source-Organization: $randomSourceOrganisation
          |Payload-Oxum: sgadfjag
          |Bagging-Date: $randomLocalDate""".stripMargin

    BagInfo.parseBagInfo(t, IOUtils.toInputStream(bagInfoString, "UTF-8")) shouldBe Left(
      InvalidBagInfo(t, List("Payload-Oxum")))
  }

  it(
    "returns a left of invalid bag info error if there is no bagging date in bag-info.txt") {
    val bagInfoString =
      s"""|External-Identifier: $createExternalIdentifier
          |Source-Organization: $randomSourceOrganisation
          |Payload-Oxum: ${randomPayloadOxum.payloadBytes}.${randomPayloadOxum.numberOfPayloadFiles}""".stripMargin

    BagInfo.parseBagInfo(t, IOUtils.toInputStream(bagInfoString, "UTF-8")) shouldBe Left(
      InvalidBagInfo(t, List("Bagging-Date")))
  }

  it(
    "returns a left of invalid bag info error if the bagging date is invalid in bag-info.txt") {
    val bagInfoString =
      s"""|External-Identifier: $createExternalIdentifier
          |Source-Organization: $randomSourceOrganisation
          |Payload-Oxum: ${randomPayloadOxum.payloadBytes}.${randomPayloadOxum.numberOfPayloadFiles}
          |Bagging-Date: sdfkjghl""".stripMargin

    BagInfo.parseBagInfo(t, IOUtils.toInputStream(bagInfoString, "UTF-8")) shouldBe Left(
      InvalidBagInfo(t, List("Bagging-Date")))
  }

  it("returns a left of invalid bag info error if bag-info.txt is empty") {
    val bagInfoString = ""

    BagInfo.parseBagInfo(t, IOUtils.toInputStream(bagInfoString, "UTF-8")) shouldBe Left(
      InvalidBagInfo(
        t,
        List("External-Identifier", "Payload-Oxum", "Bagging-Date")))
  }

}
