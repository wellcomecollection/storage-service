package uk.ac.wellcome.platform.archive.bagverifier.services

import uk.ac.wellcome.platform.archive.bagverifier.models.VerificationIncompleteSummary
import uk.ac.wellcome.platform.archive.common.storage.models.IngestFailed
import uk.ac.wellcome.storage.{Location, Prefix}

trait ReplicatedBagVerifierTestCases[BagLocation <: Location, BagPrefix <: Prefix[BagLocation],Namespace] extends BagVerifierTestCases[ReplicatedBagVerifier[BagLocation,BagPrefix],ReplicatedBagRoots[BagLocation,BagPrefix],BagLocation, BagPrefix,Namespace] {
  override def createBagRoot(bagRoot: BagPrefix, scrBagRoot: Option[BagPrefix]): ReplicatedBagRoots[BagLocation, BagPrefix] = ReplicatedBagRoots(bagRoot, scrBagRoot.getOrElse(bagRoot))

  it("fails a bag if it doesn't match original tag manifest") {
    withNamespace { implicit namespace =>
      withTypedStore { implicit typedStore =>
        val space = createStorageSpace

        val (srcBagObjects, srcBagRoot, _) = createBagContentsWith(
          space = space,
          payloadFileCount = payloadFileCount
        )

        val (bagObjects, bagRoot, bagInfo) = createBagContentsWith(
          space = space,
          payloadFileCount = payloadFileCount
        )
        uploadBagObjects(bagRoot = srcBagRoot, objects = srcBagObjects)
        uploadBagObjects(bagRoot = bagRoot, objects = bagObjects)

        val ingestStep =
          withVerifier(namespace) {
            _.verify(
              ingestId = createIngestID,
              bagRoot = createBagRoot(bagRoot, Some(srcBagRoot)),
              space = space,
              externalIdentifier = bagInfo.externalIdentifier
            )
          }

        val result = ingestStep.success.get

        result shouldBe a[IngestFailed[_]]
        result.summary shouldBe a[VerificationIncompleteSummary]

        result.maybeUserFacingMessage shouldNot be(defined)
      }
    }
  }
}
