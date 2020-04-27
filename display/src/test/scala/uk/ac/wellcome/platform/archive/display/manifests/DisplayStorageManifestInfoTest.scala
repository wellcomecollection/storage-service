package uk.ac.wellcome.platform.archive.display.manifests

import java.time.format.DateTimeFormatter

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.generators.BagInfoGenerators

class DisplayStorageManifestInfoTest
    extends FunSpec
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
