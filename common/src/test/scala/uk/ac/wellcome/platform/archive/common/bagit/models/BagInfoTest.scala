//package uk.ac.wellcome.platform.archive.common.bagit.models
//
//import java.io.InputStream
//
//import org.scalatest.{EitherValues, FunSpec, Matchers, TryValues}
//import uk.ac.wellcome.platform.archive.common.bagit.models.error.InvalidBagInfo
//import uk.ac.wellcome.platform.archive.common.generators.{
//  ExternalIdentifierGenerators,
//  PayloadOxumGenerators
//}
//import uk.ac.wellcome.storage.streaming.Codec._
//
//import scala.util.Success
//
//class BagInfoTest
//    extends FunSpec
//    with Matchers
//    with EitherValues
//    with TryValues
//    with ExternalIdentifierGenerators
//    with PayloadOxumGenerators {
//
//
//  it(
//    "returns a left of invalid bag info error if there is no external identifier in bag-info.txt"
//  ) {
//    val bagInfoString =
//      s"""|Source-Organization: $randomSourceOrganisation
//          |Payload-Oxum: ${createPayloadOxum.payloadBytes}.${createPayloadOxum.numberOfPayloadFiles}
//          |Bagging-Date: $randomLocalDate""".stripMargin
//
//    BagInfo.create(toInputStream(bagInfoString)) shouldBe Left(
//      InvalidBagInfo(List("External-Identifier"))
//    )
//  }
//
//  it(
//    "returns a left of invalid bag info error if there is no payload-oxum in bag-info.txt"
//  ) {
//    val bagInfoString =
//      s"""|External-Identifier: $createExternalIdentifier
//          |Source-Organization: $randomSourceOrganisation
//          |Bagging-Date: $randomLocalDate""".stripMargin
//
//    BagInfo.create(toInputStream(bagInfoString)) shouldBe Left(
//      InvalidBagInfo(List("Payload-Oxum"))
//    )
//  }
//
//  it(
//    "returns a left of invalid bag info error if the payload-oxum is invalid in bag-info.txt"
//  ) {
//    val bagInfoString =
//      s"""|External-Identifier: $createExternalIdentifier
//          |Source-Organization: $randomSourceOrganisation
//          |Payload-Oxum: sgadfjag
//          |Bagging-Date: $randomLocalDate""".stripMargin
//
//    BagInfo.create(toInputStream(bagInfoString)) shouldBe Left(
//      InvalidBagInfo(List("Payload-Oxum"))
//    )
//  }
//
//  it(
//    "returns a left of invalid bag info error if there is no bagging date in bag-info.txt"
//  ) {
//    val bagInfoString =
//      s"""|External-Identifier: $createExternalIdentifier
//          |Source-Organization: $randomSourceOrganisation
//          |Payload-Oxum: ${createPayloadOxum.payloadBytes}.${createPayloadOxum.numberOfPayloadFiles}""".stripMargin
//
//    BagInfo.create(toInputStream(bagInfoString)) shouldBe Left(
//      InvalidBagInfo(List("Bagging-Date"))
//    )
//  }
//
//  it(
//    "returns a left of invalid bag info error if the bagging date is invalid in bag-info.txt"
//  ) {
//    val bagInfoString =
//      s"""|External-Identifier: $createExternalIdentifier
//          |Source-Organization: $randomSourceOrganisation
//          |Payload-Oxum: ${createPayloadOxum.payloadBytes}.${createPayloadOxum.numberOfPayloadFiles}
//          |Bagging-Date: sdfkjghl""".stripMargin
//
//    BagInfo.create(toInputStream(bagInfoString)) shouldBe Left(
//      InvalidBagInfo(List("Bagging-Date"))
//    )
//  }
//
//  it("returns a left of invalid bag info error if bag-info.txt is empty") {
//    val bagInfoString = ""
//
//    BagInfo.create(toInputStream(bagInfoString)) shouldBe Left(
//      InvalidBagInfo(List("External-Identifier", "Payload-Oxum", "Bagging-Date"))
//    )
//  }
//
//}
