package uk.ac.wellcome.platform.archive.common.bagit.services

import java.io.InputStream

import org.scalatest.{Assertion, EitherValues, TryValues}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagInfo,
  ExternalDescription
}
import uk.ac.wellcome.platform.archive.common.generators.{
  ExternalIdentifierGenerators,
  PayloadOxumGenerators
}
import uk.ac.wellcome.storage.streaming.Codec._

import scala.util.Success

class BagInfoParserTest
    extends AnyFunSpec
    with Matchers
    with EitherValues
    with TryValues
    with ExternalIdentifierGenerators
    with PayloadOxumGenerators {

  describe("extracts a BagInfo from a valid bag-info.txt") {
    it("a bag-info.txt with only required fields") {
      val externalIdentifier = createExternalIdentifier
      val payloadOxum = createPayloadOxum
      val baggingDate = randomLocalDate

      val bagInfoString =
        s"""|External-Identifier: $externalIdentifier
            |Payload-Oxum: $payloadOxum
            |Bagging-Date: $baggingDate
            |""".stripMargin

      BagInfoParser.create(toInputStream(bagInfoString)).success.value shouldBe
        BagInfo(externalIdentifier, payloadOxum, baggingDate)
    }

    it("a bag-info.txt with required and optional fields") {
      val externalIdentifier = createExternalIdentifier
      val payloadOxum = createPayloadOxum
      val baggingDate = randomLocalDate
      val sourceOrganisation = Some(randomSourceOrganisation)
      val externalDescription = Some(randomExternalDescription)
      val internalSenderIdentifier = Some(randomInternalSenderIdentifier)
      val internalSenderDescription = Some(randomInternalSenderDescription)

      val bagInfoString =
        s"""|External-Identifier: $externalIdentifier
            |Payload-Oxum: $payloadOxum
            |Bagging-Date: $baggingDate
            |Source-Organization: ${sourceOrganisation.get}
            |External-Description: ${externalDescription.get}
            |Internal-Sender-Identifier: ${internalSenderIdentifier.get}
            |Internal-Sender-Description: ${internalSenderDescription.get}
            |""".stripMargin

      BagInfoParser.create(toInputStream(bagInfoString)).success.value shouldBe
        BagInfo(
          externalIdentifier,
          payloadOxum,
          baggingDate,
          sourceOrganisation,
          externalDescription,
          internalSenderIdentifier,
          internalSenderDescription
        )
    }

    it("it ignores other metadata fields in the bag-info.txt") {
      val bagInfoString =
        s"""|External-Identifier: $createExternalIdentifier
            |Payload-Oxum: $createPayloadOxum
            |Bagging-Date: $randomLocalDate
            |Bag-Count: 1 of 15
            |""".stripMargin

      BagInfoParser.create(toInputStream(bagInfoString)) shouldBe a[Success[_]]
    }

    it("de-duplicates fields if they have the same value") {
      val externalIdentifier = createExternalIdentifier

      val bagInfoString =
        s"""|External-Identifier: $externalIdentifier
            |External-Identifier: $externalIdentifier
            |Payload-Oxum: $createPayloadOxum
            |Bagging-Date: $randomLocalDate
            |""".stripMargin

      val bagInfo =
        BagInfoParser.create(toInputStream(bagInfoString)).success.value
      bagInfo.externalIdentifier shouldBe externalIdentifier
    }

    it("handles a colon in a field value") {
      val bagInfoString =
        s"""|External-Identifier: $createExternalIdentifier
            |Payload-Oxum: $createPayloadOxum
            |Bagging-Date: $randomLocalDate
            |External-Description: John Bishop King: diary 1855-1866.
            |""".stripMargin

      val bagInfo =
        BagInfoParser.create(toInputStream(bagInfoString)).success.value

      bagInfo.externalDescription shouldBe Some(
        ExternalDescription("John Bishop King: diary 1855-1866.")
      )
    }
  }

  describe("rejects a bag-info.txt that is missing required fields") {
    it("missing the External-Identifier field") {
      val bagInfoString =
        s"""|Payload-Oxum: $createPayloadOxum
            |Bagging-Date: $randomLocalDate
            |""".stripMargin

      assertIsError(
        bagInfoString,
        errorMessage = "Missing key in bag-info.txt: External-Identifier"
      )
    }

    it("missing the Payload-Oxum field") {
      val bagInfoString =
        s"""|External-Identifier: $createExternalIdentifier
            |Bagging-Date: $randomLocalDate
            |""".stripMargin

      assertIsError(
        bagInfoString,
        errorMessage = "Missing key in bag-info.txt: Payload-Oxum"
      )
    }

    it("missing the Bagging-Date field") {
      val bagInfoString =
        s"""|External-Identifier: $createExternalIdentifier
            |Payload-Oxum: $createPayloadOxum
            |""".stripMargin

      assertIsError(
        bagInfoString,
        errorMessage = "Missing key in bag-info.txt: Bagging-Date"
      )
    }
  }

  describe("rejects a bag-info.txt that has malformed fields") {
    it("if a line does not have a label") {
      val bagInfoString =
        s"""|: value
            |""".stripMargin

      assertIsError(
        bagInfoString,
        errorMessage =
          "Unable to parse the following lines in bag-info.txt: : value"
      )
    }

    it("if the External-Identifier field has an illegal value") {
      // External identifiers are not allowed to start with slashes
      val bagInfoString =
        s"""|External-Identifier: /a
            |Payload-Oxum: $createPayloadOxum
            |Bagging-Date: $randomLocalDate
            |""".stripMargin

      assertIsError(
        bagInfoString,
        errorMessage =
          "Unable to parse External-Identifier in bag-info.txt: External identifier cannot start with a slash"
      )
    }

    it("if the Payload-Oxum field has an illegal value") {
      val bagInfoString =
        s"""|External-Identifier: $createExternalIdentifier
            |Payload-Oxum: 1.2.3
            |Bagging-Date: $randomLocalDate
            |""".stripMargin

      assertIsError(
        bagInfoString,
        errorMessage = "Unable to parse Payload-Oxum in bag-info.txt: 1.2.3"
      )
    }

    it("if the Bagging-Date field has an illegal value") {
      val bagInfoString =
        s"""|External-Identifier: $createExternalIdentifier
            |Payload-Oxum: $createPayloadOxum
            |Bagging-Date: not_a_real_date
            |""".stripMargin

      assertIsError(
        bagInfoString,
        errorMessage =
          "Unable to parse Bagging-Date in bag-info.txt: Text 'not_a_real_date' could not be parsed at index 0"
      )
    }
  }

  it("rejects a bag with multiple, distinct values for the same label") {
    val bagInfoString =
      s"""|External-Identifier: 1
          |External-Identifier: 2
          |Payload-Oxum: $createPayloadOxum
          |Bagging-Date: $randomLocalDate
          |""".stripMargin

    assertIsError(
      bagInfoString,
      errorMessage =
        "Multiple values for External-Identifier in bag-info.txt: 1, 2"
    )
  }

  describe("handling empty values") {
    it("allows an empty External-Description") {
      val bagInfoString =
        s"""|External-Identifier: $createExternalIdentifier
            |Payload-Oxum: $createPayloadOxum
            |Bagging-Date: $randomLocalDate
            |External-Description:
            |""".stripMargin

      val bagInfo =
        BagInfoParser.create(toInputStream(bagInfoString)).success.value
      bagInfo.externalDescription shouldBe Some(ExternalDescription(""))
    }

    it("allows an External-Description with extra whitespace") {
      // Based on a real bag; digitised/b31366752/v8 in the staging service
      val bagInfoString =
        "External-Description: \nExternal-Identifier: b31366752\nPayload-Oxum: 1777806903.3821\nBagging-Date: 2019-09-18"

      val bagInfo =
        BagInfoParser.create(toInputStream(bagInfoString)).success.value
      bagInfo.externalDescription shouldBe Some(ExternalDescription(""))
    }

    it("blocks an empty External-Identifier") {
      val bagInfoString =
        s"""|External-Identifier:
            |Payload-Oxum: $createPayloadOxum
            |Bagging-Date: $randomLocalDate
            |""".stripMargin

      assertIsError(
        bagInfoString,
        errorMessage =
          "Unable to parse External-Identifier in bag-info.txt: External identifier cannot be empty"
      )
    }
  }

  private def assertIsError(
    bagInfoString: String,
    errorMessage: String
  ): Assertion = {
    val err = BagInfoParser.create(toInputStream(bagInfoString)).failed.get
    err shouldBe a[RuntimeException]
    err.getMessage shouldBe errorMessage
  }

  def toInputStream(s: String): InputStream =
    stringCodec.toStream(s).right.value
}
