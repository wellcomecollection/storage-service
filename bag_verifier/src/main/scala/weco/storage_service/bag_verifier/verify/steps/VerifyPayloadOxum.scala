package weco.storage_service.bag_verifier.verify.steps

import weco.storage_service.bag_verifier.fixity.FileFixityCorrect
import weco.storage_service.bag_verifier.models.BagVerifierError
import weco.storage_service.bagit.models.Bag

trait VerifyPayloadOxum {
  def verifyPayloadOxumFileCount(bag: Bag): Either[BagVerifierError, Unit] = {
    val payloadOxumCount = bag.info.payloadOxum.numberOfPayloadFiles
    val manifestCount = bag.payloadManifest.entries.size

    if (payloadOxumCount != manifestCount) {
      Left(
        BagVerifierError(
          s"Payload-Oxum has the wrong number of payload files: $payloadOxumCount, but bag manifest has $manifestCount"
        )
      )
    } else {
      Right(())
    }
  }

  def verifyPayloadOxumFileSize(
    bag: Bag,
    locations: Seq[FileFixityCorrect[_]]
  ): Either[BagVerifierError, Unit] = {
    // The Payload-Oxum octetstream sum only counts the size of files in the payload,
    // not manifest files such as the bag-info.txt file.
    // We need to filter those out.
    val payloadPaths = bag.payloadManifest.entries.keys.toSet

    val actualSize =
      locations
        .filter { loc =>
          payloadPaths.contains(loc.expectedFileFixity.path)
        }
        .map { _.size }
        .sum

    val expectedSize = bag.info.payloadOxum.payloadBytes

    if (actualSize == expectedSize) {
      Right(())
    } else {
      Left(
        BagVerifierError(
          s"Payload-Oxum has the wrong octetstream sum: $expectedSize bytes, but bag actually contains $actualSize bytes"
        )
      )
    }
  }
}
