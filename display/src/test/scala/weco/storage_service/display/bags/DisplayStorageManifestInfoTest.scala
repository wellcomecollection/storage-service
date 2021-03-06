package weco.storage_service.display.bags

import java.time.format.DateTimeFormatter

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.storage_service.generators.BagInfoGenerators

class DisplayStorageManifestInfoTest
    extends AnyFunSpec
    with Matchers
    with BagInfoGenerators {

  it("transforms a BagInfo with all fields into a DisplayBagInfo") {
    val bagInfo = createBagInfo
    ResponseDisplayBagInfo(bagInfo) shouldBe ResponseDisplayBagInfo(
      bagInfo.externalIdentifier.underlying,
      s"${bagInfo.payloadOxum.payloadBytes}.${bagInfo.payloadOxum.numberOfPayloadFiles}",
      bagInfo.baggingDate.format(DateTimeFormatter.ISO_LOCAL_DATE),
      Some(bagInfo.sourceOrganisation.get.underlying),
      Some(bagInfo.externalDescription.get.underlying),
      Some(bagInfo.internalSenderIdentifier.get.underlying),
      Some(bagInfo.internalSenderDescription.get.underlying)
    )
  }

}
